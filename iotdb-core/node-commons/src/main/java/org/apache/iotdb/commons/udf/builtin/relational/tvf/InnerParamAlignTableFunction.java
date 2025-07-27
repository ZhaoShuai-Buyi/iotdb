package org.apache.iotdb.commons.udf.builtin.relational.tvf;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.TableSessionBuilder;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InnerParamAlignTableFunction implements TableFunction {

  private final String TABLE_LIST = "table";
  private final String ALIGN_COLUMN = "alignColumn";
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
            ScalarParameterSpecification.builder().name(AIRCRAFT).type(Type.STRING).build());
  }

  @Override
  public TableFunctionAnalysis analyze(Map<String, Argument> arguments) throws UDFException {

    ScalarArgument tableList = (ScalarArgument) arguments.get(TABLE_LIST);
    // ScalarArgument aircraftList = (ScalarArgument) arguments.get(AIRCRAFT);
    String[] tableListValue = analyzeSplit(String.valueOf(tableList.getValue()));
    // String[] aircraftListValue = analyzeAircraft(aircraftList);
    DescribedSchema.Builder schemaBuilder = DescribedSchema.builder();
    schemaBuilder.addField("time", Type.TIMESTAMP);
    schemaBuilder.addField("aircraft", Type.STRING);
    for (int i = 0; i < tableListValue.length; i++) {
      // todo: 类型是否要判断
      schemaBuilder.addField("value" + (i + 1), Type.FLOAT);
    }
    DescribedSchema schema = schemaBuilder.build();

    MapTableFunctionHandle handle =
            new MapTableFunctionHandle.Builder()
                    .addProperty(TABLE_LIST, ((ScalarArgument) arguments.get(TABLE_LIST)).getValue())
                    .addProperty(ALIGN_COLUMN, ((ScalarArgument) arguments.get(ALIGN_COLUMN)).getValue())
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

    private String[] tableListValue;
    private final String[] tableListName;
    // todo:高频向低频对齐
    private final String alignColumn;
    private final String[] aircraftListValue;
    private int sIndex = -1;

    private CacheLine[] cacheLines = null;

    private boolean finish = false;
    //private boolean isInit = false;

    private ITableSession[] sessions = null;
    private SessionDataSet[] sessionDataSets = null;
    private SessionDataSet.DataIterator[] dataIterators = null;

    private ITableSession countSession = null;

    ParamAlignProcessor(String tableListValue, String alignColumn, String aircraftListValue) {
      this.tableListValue = analyzeSplit(tableListValue);
      this.tableListName = getTableNameList(this.tableListValue);
      this.alignColumn = alignColumn;
      this.aircraftListValue = analyzeSplit(aircraftListValue);
      int size = this.tableListValue.length;
      this.sessions = new ITableSession[size];
      this.sessionDataSets = new SessionDataSet[size];
      this.dataIterators = new SessionDataSet.DataIterator[size];
      this.cacheLines = new CacheLine[size];
    }

    @Override
    public void beforeStart() {
      tableListValue = getSql(tableListValue, aircraftListValue);
      try {
        countSession =
                new TableSessionBuilder()
                        .nodeUrls(Collections.singletonList("127.0.0.1:6667"))
                        .username("root")
                        .password("root")
                        .database("b777")
                        .build();
        //private long standardTime = -1;
        //private long standardFrequency = 0;
        Map<Integer, Long> countList = new HashMap<>();
        long frequence = -1;
        for (int i = 0; i < tableListValue.length; i++) {
          SessionDataSet countSessionDataSet =
                  countSession.executeQueryStatement("select count(*) from " + tableListName[i]);
          RowRecord countRowRecord = countSessionDataSet.next();
          Field field = countRowRecord.getField(0);
          long countValue = field.getLongV();
          countList.put(i, countValue);
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
        //maxCount = countList.get(sIndex);
        for (int i = 0; i < tableListValue.length; i++) {
          sessions[i] =
                  new TableSessionBuilder()
                          .nodeUrls(Collections.singletonList("127.0.0.1:6667"))
                          .username("root")
                          .password("root")
                          .database("b777")
                          .build();
          sessionDataSets[i] = sessions[i].executeQueryStatement(tableListValue[i]);
          dataIterators[i] = sessionDataSets[i].iterator();
          CacheLine cacheLine = new CacheLine(dataIterators[i]);
          cacheLine.next();
          cacheLines[i] = cacheLine;
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

        while (dataIterators[sIndex].next()) {
          long currentStandardTime = dataIterators[sIndex].getLong("time");
          float currentValue = dataIterators[sIndex].getFloat("value");
          Binary currentAir = dataIterators[sIndex].getBlob("aircraft/tail");
          columnBuilders.get(0).writeLong(currentStandardTime);
          columnBuilders.get(1).writeBinary(currentAir);
          columnBuilders.get(sIndex + 2).writeFloat(currentValue);
          for (int index = 0; index < tableListValue.length; index++) {
            if (index == sIndex) continue;
            while (cacheLines[index].hasNext()) {
              long firstDiff = Math.abs(cacheLines[index].getT1() - currentStandardTime);
              long secondDiff = cacheLines[index].hasNext() ? Math.abs(cacheLines[index].getT2() - currentStandardTime) : Long.MAX_VALUE;
              if (firstDiff <= secondDiff) {
                columnBuilders.get(index + 2).writeFloat(cacheLines[index].getValue1());
                break;
              } else {
                cacheLines[index].next();
              }
            }
            if (!cacheLines[index].hasNext()) {
              columnBuilders.get(index + 2).appendNull();
            }
          }
          alignRows++;
          if (alignRows >= 1000) {
            return;
          }
        }
        finish = true;
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        // } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void beforeDestroy() {
      try {
        countSession.close();
        for (int i = 0; i < tableListValue.length; i++) {
          sessionDataSets[i].close();
          sessions[i].close();
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

  // 查询语句为 select * from n21 where value > 10 and time xxx 这种类型
  // 最后关心解析的最优实现
  private String[] analyzeSplit(String sourceList) {
    String[] listValue = sourceList.split("\\),\\(");
    listValue[0] = listValue[0].substring(1);
    listValue[listValue.length - 1] =
            listValue[listValue.length - 1].substring(0, listValue[listValue.length - 1].length() - 1);
    return listValue;
  }

  private String[] getSql(String[] tableListValue, String[] aircraftListValue) {
    String[] sqlList = new String[tableListValue.length];
    // todo：需要添加其他航班信息
    String[] eachAircraft = aircraftListValue[0].split(",");
    for (int hb = 0; hb < tableListValue.length; hb++) {
      String first = eachAircraft[0];
      String last = eachAircraft[1];
      String air = eachAircraft[2];
      String sql =
              tableListValue[hb]
                      + " where \"aircraft/tail\" = "
                      + air
                      + " and time >= "
                      + first
                      + " and time <= "
                      + last;
      sqlList[hb] = sql;
    }
    return sqlList;
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
}
