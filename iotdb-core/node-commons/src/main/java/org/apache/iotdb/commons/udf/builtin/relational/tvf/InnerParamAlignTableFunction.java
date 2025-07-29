package org.apache.iotdb.commons.udf.builtin.relational.tvf;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.TableSessionBuilder;
import org.apache.iotdb.udf.api.exception.UDFArgumentNotValidException;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.TableFunction;
import org.apache.iotdb.udf.api.relational.table.MapTableFunctionHandle;
import org.apache.iotdb.udf.api.relational.table.TableFunctionAnalysis;
import org.apache.iotdb.udf.api.relational.table.TableFunctionHandle;
import org.apache.iotdb.udf.api.relational.table.TableFunctionProcessorProvider;
import org.apache.iotdb.udf.api.relational.table.argument.Argument;
import org.apache.iotdb.udf.api.relational.table.argument.DescribedSchema;
import org.apache.iotdb.udf.api.relational.table.argument.ScalarArgument;
import org.apache.iotdb.udf.api.relational.table.processor.TableFunctionLeafProcessor;
import org.apache.iotdb.udf.api.relational.table.specification.ParameterSpecification;
import org.apache.iotdb.udf.api.relational.table.specification.ScalarParameterSpecification;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.utils.Binary;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class InnerParamAlignTableFunction implements TableFunction {

  private final String TABLE_LIST = "table";
  private final String ALIGN_COLUMN = "alignColumn";
  private final String FILL = "fill";
  private final String AIRCRAFT = "aircraft";

  @Override
  public List<ParameterSpecification> getArgumentsSpecifications() {
    return Arrays.asList(
            ScalarParameterSpecification.builder().name(TABLE_LIST).type(Type.STRING).build(),
            ScalarParameterSpecification.builder()
                    .name(ALIGN_COLUMN)
                    .type(Type.STRING)
                    .defaultValue("default")
                    .build(),
            ScalarParameterSpecification.builder()
                    .name(FILL)
                    .type(Type.STRING)
                    .defaultValue("previous")
                    .build(),
            ScalarParameterSpecification.builder().name(AIRCRAFT).type(Type.STRING).build());
  }

  @Override
  public TableFunctionAnalysis analyze(Map<String, Argument> arguments) throws UDFException {

    ScalarArgument tableList = (ScalarArgument) arguments.get(TABLE_LIST);
    String[] tableListValue = analyzeSplit(String.valueOf(tableList.getValue()));
    DescribedSchema.Builder schemaBuilder = DescribedSchema.builder();
    schemaBuilder.addField("time", Type.TIMESTAMP);
    schemaBuilder.addField("aircraft", Type.STRING);
    String[] tableNames = getTableNameList(tableListValue);
    for (int i = 0; i < tableNames.length; i++) {
      // todo: 类型判断
      schemaBuilder.addField(tableNames[i], Type.FLOAT);
    }
    DescribedSchema schema = schemaBuilder.build();

    // 判断 alignColumn 和 fill，填写有误给出报错提示
    ScalarArgument alignColumn = (ScalarArgument) arguments.get(ALIGN_COLUMN);
    ScalarArgument fill = (ScalarArgument) arguments.get(FILL);
    boolean alignValidate = false;
    if ("default".equals(alignColumn.getValue())) alignValidate = true;
    for (String tableName : tableNames) {
      if (alignColumn.getValue().equals(tableName)) alignValidate = true;
    }
    if (!alignValidate) throw new UDFArgumentNotValidException("alignColumn support default or tableName");
    if (!"previous".equals(fill.getValue())) {
      throw new UDFArgumentNotValidException("fill only support previous");
    }
    MapTableFunctionHandle handle =
            new MapTableFunctionHandle.Builder()
                    .addProperty(TABLE_LIST, ((ScalarArgument) arguments.get(TABLE_LIST)).getValue())
                    .addProperty(ALIGN_COLUMN, ((ScalarArgument) arguments.get(ALIGN_COLUMN)).getValue())
                    .addProperty(FILL, ((ScalarArgument) arguments.get(FILL)).getValue())
                    .addProperty(AIRCRAFT, ((ScalarArgument) arguments.get(AIRCRAFT)).getValue())
                    .build();

    return TableFunctionAnalysis.builder().properColumnSchema(schema).handle(handle).build();
  }

  @Override
  public TableFunctionHandle createTableFunctionHandle() {
    return new MapTableFunctionHandle();
  }

  @Override
  public TableFunctionProcessorProvider getProcessorProvider(
          TableFunctionHandle tableFunctionHandle) {
    return new TableFunctionProcessorProvider() {
      @Override
      public TableFunctionLeafProcessor getSplitProcessor() {
        return new ParamAlignProcessor(
                (String) (((MapTableFunctionHandle) tableFunctionHandle).getProperty(TABLE_LIST)),
                (String) (((MapTableFunctionHandle) tableFunctionHandle).getProperty(ALIGN_COLUMN)),
                (String) (((MapTableFunctionHandle) tableFunctionHandle).getProperty(AIRCRAFT)));
      }
    };
  }

  private class ParamAlignProcessor implements TableFunctionLeafProcessor {

    private String[][] allTable;
    private String[] tableListValue;
    private final String[] tableListName;
    private final String alignColumn;
    private final String[] aircraftListValue;
    private int airCount = 1;
    private int curCount = 0;
    private int sIndex = -1;

    private CacheLine[][] cacheLines = null;
    //private TimeAndValue[] linearCache = null;

    private boolean finish = false;

    private ITableSession[][] allSessions = null;
    private SessionDataSet[][] allSessionDataSets = null;
    private SessionDataSet.DataIterator[][] allDataIterators = null;

    private ITableSession countSession = null;

    ParamAlignProcessor(String tableListValue, String alignColumn, String aircraftListValue) {
      this.tableListValue = analyzeSplit(tableListValue);
      this.tableListName = getTableNameList(this.tableListValue);
      this.alignColumn = alignColumn;
      this.aircraftListValue = analyzeSplit(aircraftListValue);
      int size = this.tableListValue.length;
      this.airCount = this.aircraftListValue.length;
      this.allTable = new String[this.airCount][size];
      this.allSessions = new ITableSession[this.airCount][size];
      this.allSessionDataSets = new SessionDataSet[this.airCount][size];
      this.allDataIterators = new SessionDataSet.DataIterator[this.airCount][size];
      this.cacheLines = new CacheLine[this.airCount][size];
      //this.linearCache = new TimeAndValue[size];
    }

    @Override
    public void beforeStart() {
      allTable = getSql(tableListValue, aircraftListValue);
      try {
        countSession =
                new TableSessionBuilder()
                        .nodeUrls(Collections.singletonList("127.0.0.1:6669"))
                        .username("root")
                        .password("root")
                        .database("b777")
                        .build();
        long frequence = -1;
        String[] firstAircraft = aircraftListValue[0].split(",");
        String first = firstAircraft[0];
        String last = firstAircraft[1];
        String airInfo = firstAircraft[2];
        for (int i = 0; i < tableListValue.length; i++) {
          SessionDataSet countSessionDataSet =
                  countSession.executeQueryStatement("select count(*) from " + tableListName[i] + " where \"aircraft/tail\" = "
                          + airInfo
                          + " and time >= "
                          + first
                          + " and time <= "
                          + last);
          RowRecord countRowRecord = countSessionDataSet.next();
          Field field = countRowRecord.getField(0);
          long countValue = field.getLongV();
          countSessionDataSet.close();
          // 等于的情况不需要更换索引，要按照索引顺序选定最高频
          if (frequence < countValue) {
            frequence = countValue;
            sIndex = i;
          }
        }
        for (int index = 0; index < tableListValue.length; index++) {
          if (tableListName[index].equals(alignColumn)) {
            sIndex = index;
            break;
          }
        }

        for (int air = 0; air < aircraftListValue.length; air++) {
          for (int i = 0; i < tableListValue.length; i++) {
            allSessions[air][i] =
                    new TableSessionBuilder()
                            .nodeUrls(Collections.singletonList("127.0.0.1:6669"))
                            .username("root")
                            .password("root")
                            .database("b777")
                            .build();
            allSessionDataSets[air][i] = allSessions[air][i].executeQueryStatement(allTable[air][i]);
            allDataIterators[air][i] = allSessionDataSets[air][i].iterator();
            CacheLine cacheLine = new CacheLine(allDataIterators[air][i]);
            cacheLine.next();
            cacheLines[air][i] = cacheLine;
          }
        }
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    // 基准时间为最高频率 or alignColumn
    @Override
    public void process(List<ColumnBuilder> columnBuilders) {
      try {
        int alignRows = 0;

        while (allDataIterators[curCount][sIndex].next()) {
          long currentStandardTime = allDataIterators[curCount][sIndex].getLong("time");
          float currentValue = allDataIterators[curCount][sIndex].getFloat("value");
          Binary currentAir = allDataIterators[curCount][sIndex].getBlob("aircraft/tail");
          columnBuilders.get(0).writeLong(currentStandardTime);
          columnBuilders.get(1).writeBinary(currentAir);
          columnBuilders.get(sIndex + 2).writeFloat(currentValue);
          for (int index = 0; index < tableListValue.length; index++) {
            if (index == sIndex) continue;
            while (cacheLines[curCount][index].hasNext()) {
              long firstDiff = Math.abs(cacheLines[curCount][index].getT1() - currentStandardTime);
              long secondDiff = cacheLines[curCount][index].hasNext() ? Math.abs(cacheLines[curCount][index].getT2() - currentStandardTime) : Long.MAX_VALUE;
              if (firstDiff <= secondDiff) {
                columnBuilders.get(index + 2).writeFloat(cacheLines[curCount][index].getValue1());
                break;
/*                                if (linearCache[index].lastTime == Long.MIN_VALUE) {

                                } else {

                                }
                                break;*/
              } else {
                cacheLines[curCount][index].next();
              }
            }
            if (!cacheLines[curCount][index].hasNext()) {
              columnBuilders.get(index + 2).writeFloat(cacheLines[curCount][index].getValue1());
            }
          }
          alignRows++;
          if (alignRows >= 1000) {
            return;
          }
        }
/*                for (int linear = 0; linear < tableListValue.length; linear++) {
                    linearCache[linear].lastTime = Long.MIN_VALUE;
                    linearCache[linear].lastValue = Float.MIN_VALUE;
                }*/
        curCount++;
        if (curCount >= airCount) {
          finish = true;
        } else {
          return;
        }
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void beforeDestroy() {
      try {
        countSession.close();
        for (int air = 0; air < airCount; air++) {
          for (int i = 0; i < tableListValue.length; i++) {
            allSessionDataSets[air][i].close();
            allSessions[air][i].close();
          }
        }
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public boolean isFinish() {
      return finish;
    }
  }

  private String[] getTableNameList(String[] tableListValue) {
    String[] tableNameList = new String[tableListValue.length];
    for (int i = 0; i < tableListValue.length; i++) {
      String source = tableListValue[i];
      String[] sourceSplit = source.split("from");
      tableNameList[i] = sourceSplit[sourceSplit.length - 1].trim();
    }
    return tableNameList;
  }

  private String[] analyzeSplit(String sourceList) {
    String[] listValue = sourceList.split("\\),\\(");
    listValue[0] = listValue[0].substring(1);
    listValue[listValue.length - 1] =
            listValue[listValue.length - 1].substring(0, listValue[listValue.length - 1].length() - 1);
    return listValue;
  }

  private String[][] getSql(String[] tableListValue, String[] aircraftListValue) {
    String[][] result = new String[aircraftListValue.length][tableListValue.length];
    for (int air = 0; air < aircraftListValue.length; air++) {
      String[] sqlList = new String[tableListValue.length];
      String[] eachAircraft = aircraftListValue[air].split(",");
      for (int hb = 0; hb < tableListValue.length; hb++) {
        String first = eachAircraft[0];
        String last = eachAircraft[1];
        String airInfo = eachAircraft[2];
        String sql =
                tableListValue[hb]
                        + " where \"aircraft/tail\" = "
                        + airInfo
                        + " and time >= "
                        + first
                        + " and time <= "
                        + last;
        sqlList[hb] = sql;
      }
      result[air] = sqlList;
    }
    return result;
  }

  public static class CacheLine {
    private long t1 = Long.MIN_VALUE;
    private long t2 = -1;
    private float value1;
    private float value2;
    private boolean finish = true;
    private SessionDataSet.DataIterator iterator;

    public CacheLine(SessionDataSet.DataIterator iterator) {
      this.iterator = iterator;
    }

    public void next() throws IoTDBConnectionException, StatementExecutionException {
      if (t1 == Long.MIN_VALUE) {
        finish = iterator.next();
        t1 = iterator.getLong("time");
        value1 = iterator.getFloat("value");
        finish = iterator.next();
        if (finish) {
          t2 = iterator.getLong("time");
          value2 = iterator.getFloat("value");
        }
      }
      finish = iterator.next();
      t1 = t2;
      value1 = value2;
      if (finish) {
        t2 = iterator.getLong("time");
        value2 = iterator.getFloat("value");
      }
    }

    public boolean hasNext() {
      return finish;
    }

    public long getT1() {
      return t1;
    }

    public long getT2() {
      return t2;
    }

    public float getValue1() {
      return value1;
    }

    public float getValue2() {
      return value2;
    }
  }

  public static class TimeAndValue {
    public long lastTime = Long.MIN_VALUE;
    public float lastValue = Float.MIN_VALUE;
  }
}
