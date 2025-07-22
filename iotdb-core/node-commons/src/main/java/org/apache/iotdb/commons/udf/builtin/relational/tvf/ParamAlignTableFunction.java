package org.apache.iotdb.commons.udf.builtin.relational.tvf;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.TableSession;
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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParamAlignTableFunction implements TableFunction {

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
      schemaBuilder.addField("value" + (i + 1), Type.DOUBLE);
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
    private long standardTime = -1;
    private Map<Integer, Long> countList = null;
    private Map<Integer, TimeAndValue> cacheFrontValue = new HashMap<>();

    private boolean finish = false;
    private boolean isInit = false;

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
    }

    @Override
    public void beforeStart() {
      tableListValue = getSql(tableListValue, aircraftListValue);
      try {
        countSession = new TableSessionBuilder()
                .nodeUrls(Collections.singletonList("127.0.0.1:6667"))
                .username("root")
                .password("root")
                .database("nh")
                .build();
        for (int i = 0; i < tableListValue.length; i++) {
          sessions[i] =
                  new TableSessionBuilder()
                          .nodeUrls(Collections.singletonList("127.0.0.1:6667"))
                          .username("root")
                          .password("root")
                          .database("nh")
                          .build();
        }
      } catch (IoTDBConnectionException e) {
        throw new RuntimeException(e);
      }
    }

    // 基准时间为最高频率 or alignColumn
    @Override
    public void process(List<ColumnBuilder> columnBuilders) {
      try {
        if (!isInit) {
          countList = new HashMap<>();
          long frequence = -1;
          for (int i = 0; i < tableListValue.length; i++) {
            SessionDataSet countSessionDataSet = countSession.executeQueryStatement("select count(*) from " + tableListName[i]);
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
            sessionDataSets[i] = sessions[i].executeQueryStatement(tableListValue[i]);
            dataIterators[i] = sessionDataSets[i].iterator();
          }
          for (int index = 0; index < tableListValue.length; index++) {
            if (tableListName[index].equals(alignColumn)) {
              sIndex = index;
            }
          }
          isInit = true;
        }


        // todo:输出多个航班对齐结果

        // todo:设置每次输出的行数
        int alignRows = 0;

        while (dataIterators[sIndex].next()) {
          if (cacheFrontValue.containsKey(sIndex)) {
            standardTime = dataIterators[sIndex].getLong("time") - cacheFrontValue.get(sIndex).getTime();
          }
          columnBuilders.get(0).writeLong(dataIterators[sIndex].getLong("time"));
          columnBuilders.get(1).writeBinary(dataIterators[sIndex].getBlob("aircraft/tail"));
          columnBuilders.get(sIndex + 2).writeDouble(dataIterators[sIndex].getDouble("value"));
/*                    columnBuilders.get(2).writeDouble(dataIterators[sIndex].getDouble("value"));
                    columnBuilders.get(3).writeDouble(dataIterators[sIndex].getDouble("value"));*/
          for (int index = 0; index < tableListValue.length; index++) {
            if (index != sIndex) {
              dataIterators[index].next();
              if (cacheFrontValue.containsKey(index)) {
                //TimeAndValue lastTV = cacheFrontValue.get(index);
                long sIndexTime = cacheFrontValue.get(sIndex).getTime();
                long currentTime = dataIterators[index].getLong("time");
                // 是否添加等于
                while (sIndexTime > currentTime && sIndexTime - currentTime > (standardTime / 2.0)) {
                  System.out.println(sIndexTime - currentTime);
                  columnBuilders.get(index + 2).writeDouble(cacheFrontValue.get(index).getValue());
                  sIndexTime -= standardTime;
                }
                double value = dataIterators[index].getDouble("value");
                columnBuilders.get(index + 2).writeDouble(value);
                TimeAndValue tv = new TimeAndValue(currentTime, value);
                cacheFrontValue.put(index, tv);
              } else {
                TimeAndValue tv = new TimeAndValue(dataIterators[index].getLong("time"), dataIterators[index].getDouble("value"));
                cacheFrontValue.put(index, tv);
              }
            }
          }
          TimeAndValue tv = new TimeAndValue(dataIterators[sIndex].getLong("time"), dataIterators[sIndex].getDouble("value"));
          cacheFrontValue.put(sIndex, tv);
        }

                long tempCount = 0;
                for (int i = 0; i < tableListValue.length; i++) {
                    if (i!= sIndex) {
                        while (tempCount < 19) {
                            columnBuilders.get(i + 2).writeDouble(0.0);
                            tempCount++;
                        }
                        tempCount = 0;
                    }
                }

        finish = true;
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        //} catch (Exception e) {
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

  private static class TimeAndValue {
    private long time;
    private double value;

    TimeAndValue(long time, double value) {
      this.time = time;
      this.value = value;
    }

    public long getTime() {
      return time;
    }

    public double getValue() {
      return value;
    }

    public void setTime (long time) {
      this.time = time;
    }

    public void setValue (double value) {
      this.value = value;
    }
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
      String sql = tableListValue[hb] + " where \"aircraft/tail\" = " + air + " and time >= " + first + " and time <= " + last;
      sqlList[hb] = sql;
    }
    return sqlList;
  }
}
