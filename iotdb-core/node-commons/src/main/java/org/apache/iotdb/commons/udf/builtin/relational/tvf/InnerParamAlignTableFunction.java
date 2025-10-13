package org.apache.iotdb.commons.udf.builtin.relational.tvf;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.TableSessionBuilder;
import org.apache.iotdb.udf.api.State;
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
import org.apache.tsfile.utils.Binary;

import java.util.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class InnerParamAlignTableFunction implements TableFunction {

  private final String TABLE_LIST = "table";
  private final String ALIGN_COLUMN = "alignColumn";
  private final String FILL = "fill";
  private final String AIRCRAFT = "aircraft";
  private final String START_TIME = "startTime";
  private final String END_TIME = "endTime";

  // 暂时不处理地点参数
  private final String LEAVE_LOCATION = "leaveLocation";
  private final String ARRIVE_LOCATION = "arriveLocation";

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
            ScalarParameterSpecification.builder().name(AIRCRAFT).type(Type.STRING).build(),
            ScalarParameterSpecification.builder().name(START_TIME).type(Type.STRING).build(),
            ScalarParameterSpecification.builder().name(END_TIME).type(Type.STRING).build(),
            ScalarParameterSpecification.builder()
                    .name(LEAVE_LOCATION)
                    .type(Type.STRING)
                    .defaultValue("null")
                    .build(),
            ScalarParameterSpecification.builder()
                    .name(ARRIVE_LOCATION)
                    .type(Type.STRING)
                    .defaultValue("null")
                    .build());
  }

  @Override
  public TableFunctionAnalysis analyze(Map<String, Argument> arguments) throws UDFException {
    // 初始化结果集
    DescribedSchema.Builder schemaBuilder = DescribedSchema.builder();
    schemaBuilder.addField("time", Type.TIMESTAMP);
    schemaBuilder.addField("aircraft", Type.STRING);
    ScalarArgument tableList = (ScalarArgument) arguments.get(TABLE_LIST);
    String[] tableNameList = getTableNameList((String) tableList.getValue());
    for (int i = 0; i < tableNameList.length; i++) {
      // todo: 类型判断，当前均为 float
      schemaBuilder.addField(tableNameList[i], Type.FLOAT);
    }
    DescribedSchema schema = schemaBuilder.build();

    // 判断参数问题
    ScalarArgument alignColumn = (ScalarArgument) arguments.get(ALIGN_COLUMN);
    ScalarArgument fill = (ScalarArgument) arguments.get(FILL);
    boolean alignValidate = false;
    if ("default".equals(alignColumn.getValue())) alignValidate = true;
    for (String tableName : tableNameList) {
      if (alignColumn.getValue().equals(tableName)) alignValidate = true;
    }
    if (!alignValidate) throw new UDFArgumentNotValidException("alignColumn support default or tableName");
    if (!"previous".equals(fill.getValue())) {
      throw new UDFArgumentNotValidException("fill only support previous");
    }

    // 返回 handle 和构建的结果集信息
    MapTableFunctionHandle handle =
            new MapTableFunctionHandle.Builder()
                    .addProperty(TABLE_LIST, ((ScalarArgument) arguments.get(TABLE_LIST)).getValue())
                    .addProperty(ALIGN_COLUMN, ((ScalarArgument) arguments.get(ALIGN_COLUMN)).getValue())
                    .addProperty(FILL, ((ScalarArgument) arguments.get(FILL)).getValue())
                    .addProperty(AIRCRAFT, ((ScalarArgument) arguments.get(AIRCRAFT)).getValue())
                    .addProperty(START_TIME, ((ScalarArgument) arguments.get(START_TIME)).getValue())
                    .addProperty(END_TIME, ((ScalarArgument) arguments.get(END_TIME)).getValue())
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
                (String) (((MapTableFunctionHandle) tableFunctionHandle).getProperty(AIRCRAFT)),
                (String) (((MapTableFunctionHandle) tableFunctionHandle).getProperty(START_TIME)),
                (String) (((MapTableFunctionHandle) tableFunctionHandle).getProperty(END_TIME)));
      }
    };
  }

  private class ParamAlignProcessor implements TableFunctionLeafProcessor {
    // 一个 session 的迭代器用于遍历 metadata 查询后的航班信息
    private ITableSession flightInfoSession = null;
    private SessionDataSet flightInfoDataSet = null;
    private SessionDataSet.DataIterator flightInfoDataIterator = null;

    // tableNameList.length 个迭代器用于跑一个航班的对齐插值处理
    private List<ITableSession> flightSession = null;
    private List<SessionDataSet> flightDataSet = null;
    private List<SessionDataSet.DataIterator> flightDataIterator = null;

    // 最高频索引
    private int sIndex = -1;
    private final String alignColumn = null; // 当前默认对齐列为最高频列，只需维护 sIndex 索引即可

    // 拼接 sql 以及所需参数
    private String[] tableNameList = null;
    private int size = 0;
    private List<String> flightSqlList = null;

    // 时间间隔记录 & time 和 value 列缓存
    private List<ArrayList<Long>> timeList = null;
    private List<ArrayList<Float>> valueList = null;
    private List<Long> rulelCandidator = null;

    // 每一个实际列的两行 time 的迭代
    private List<CacheLine> cacheLines = null;

    // 整体结束条件
    private boolean finish = false;

    // 航班总信息
    private String aircraft = null;
    private String startTime = null;
    private String endTime = null;

    ParamAlignProcessor(String tableList, String alignColumn, String aircraft, String startTime, String endTime) {
      this.aircraft = aircraft;
      this.startTime = startTime;
      this.endTime = endTime;

      this.tableNameList = getTableNameList(tableList);
      this.size = this.tableNameList.length;

      this.flightSession = new ArrayList<ITableSession>(size);
      this.flightDataSet = new ArrayList<SessionDataSet>(size);
      this.flightDataIterator = new ArrayList<SessionDataSet.DataIterator>(size);

      this.flightSqlList = new ArrayList<String>(size);

      this.timeList = new ArrayList<>(size);
      this.valueList = new ArrayList<>(size);

      this.rulelCandidator = new ArrayList<>(size);

      this.cacheLines = new ArrayList<CacheLine>(size);
    }

    @Override
    public void beforeStart() {
      try {
        // 获取 metadata 迭代器
        flightInfoSession = new TableSessionBuilder()
                .nodeUrls(Collections.singletonList("127.0.0.1:6669"))
                .username("root")
                .password("root")
                .database("b777")
                .build();
        flightInfoDataSet = flightInfoSession.executeQueryStatement("select * from flight_metadata where \"aircraft/tail\" = '" + aircraft + "' and \"flight/datetime/startrecordingdatetime\" >= '" + startTime + "' and \"flight/datetime/endrecordingdatetime\" <= '" + endTime + "'"); // 后续可选加入地点过滤条件
        flightInfoDataIterator = flightInfoDataSet.iterator();
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    // 每个 process 提交的契机都是完成一个航班的处理，需要处理资源获取和释放
    @Override
    public void process(List<ColumnBuilder> columnBuilders) {
      try {
        // 初始化查询语句，遍历并缓存所有的 time 和 value，拿到 sIndex 或者基准列

        // 判断是否还有航班
        if(!flightInfoDataIterator.next()) {
          finish = true;
          return;
        }

        // 重新初始化那一堆list
        for (int i = 0; i < size; i++) {
          this.timeList.add(new ArrayList<>());
          this.valueList.add(new ArrayList<>());
        }
        for (int i = 0; i < size; i++) {
          rulelCandidator.add(Long.MAX_VALUE);
        }

        // 当前航班所有迭代器
        String currentFlightStartTime = flightInfoDataIterator.getString("flight/datetime/startrecordingdatetime");
        if (currentFlightStartTime.endsWith("Z")) {
          currentFlightStartTime = currentFlightStartTime.substring(0, currentFlightStartTime.length() - 1);
        }
        String currentFlightEndTime = flightInfoDataIterator.getString("flight/datetime/endrecordingdatetime");
        if (currentFlightEndTime.endsWith("Z")) {
          currentFlightEndTime = currentFlightEndTime.substring(0, currentFlightEndTime.length() - 1);
        }
        long standardTime = parseTimeToNanos(currentFlightStartTime);
        long endding = parseTimeToNanos(currentFlightEndTime);
        for (int i = 0; i < size; i++) {
          //flightSqlList.add("select time, value from " + tableNameList[i] + " where \"aircraft/tail\" = '" + aircraft + "' and time >= " + currentFlightStartTime + " and time <= " + currentFlightEndTime);
          flightSqlList.add("select time, value from " + tableNameList[i] + " where \"aircraft/tail\" = '" + aircraft + "' and time >= " + standardTime + " and time <= " + endding);
          flightSession.add(new TableSessionBuilder().nodeUrls(Collections.singletonList("127.0.0.1:6669")).username("root").password("root").database("b777").build());
          flightDataSet.add(flightSession.get(i).executeQueryStatement(flightSqlList.get(i)));
          flightDataIterator.add(flightDataSet.get(i).iterator());
        }

        // 初始化 time 和 value 列，找基准列
        for (int i = 0; i < size; i++) {
          long last = -1;
          while (flightDataIterator.get(i).next()) {
            long cur = flightDataIterator.get(i).getLong("time");
            float val = flightDataIterator.get(i).getFloat("value");
            timeList.get(i).add(cur);
            valueList.get(i).add(val);
            if (last != -1) rulelCandidator.set(i, Math.min(Math.abs(cur - last), rulelCandidator.get(i)));
            last = cur;
          }
        }

        // 对比 ruleCandidator。找出并计算基准列
        long ruleTime = Long.MAX_VALUE;
        for (int i = 0; i < size; i++) {
          // 等于不交换，只取第一个最高频
          if (rulelCandidator.get(i) < ruleTime) {
            ruleTime = rulelCandidator.get(i);
          }
        }

        // 直接从开始时间按照 ruleTime 递增构造对齐结果集，其中，值写入中间对齐结果集，时间直接写入 columnBuilder
        List<List<Float>> tempResult = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
          tempResult.add(new ArrayList<>());
        }

        List<Iterator<Long>> timeIterators = new ArrayList<>(size);
        List<Iterator<Float>> valueIterators = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
          timeIterators.add(timeList.get(i).iterator());
          valueIterators.add(valueList.get(i).iterator());
          CacheLine cacheLine = new CacheLine(timeIterators.get(i), valueIterators.get(i));
          cacheLines.add(cacheLine);
          // 初始化第一个数据点
          if (cacheLine.hasNext()) {
            cacheLine.next();
          }
        }
        Binary aircraftOnly = new Binary(aircraft.getBytes());

        // 对齐。补 null
        while (standardTime <= endding) {
          columnBuilders.get(0).writeLong(standardTime);
          columnBuilders.get(1).writeBinary(aircraftOnly);
          for (int i = 0; i < size; i++) {
            if (cacheLines.get(i).hasNext()) {
              long firstDiff = Math.abs(cacheLines.get(i).getT1() - standardTime);
              long secondDiff = cacheLines.get(i).hasNext() ? Math.abs(cacheLines.get(i).getT2() - standardTime) : Long.MAX_VALUE;
              // 貌似除以2更合理
              if (firstDiff <= secondDiff && firstDiff <= (ruleTime / 2.0)) {
                tempResult.get(i).add(cacheLines.get(i).getValue1());
                cacheLines.get(i).next();
              } else {
                tempResult.get(i).add(null);
              }
            } else {
              tempResult.get(i).add(cacheLines.get(i).getValue1());
            }
          }
          standardTime += ruleTime;
        }
        // 填充
        for (int i = 0; i < size; i++) {
          float lastValue = 0.0f;
          for (Float value : tempResult.get(i)) {
            if (value != null) {
              columnBuilders.get(i + 2).writeFloat(value);
              lastValue = value;
            } else {
              columnBuilders.get(i + 2).writeFloat(lastValue);
            }
          }
        }
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        throw new RuntimeException(e);
      } finally {
        try {
          if (flightDataSet != null) {
            for (int i = 0; i < flightDataSet.size(); i++) {
              if (flightDataSet.get(i) != null) {
                flightDataSet.get(i).close();
              }
            }
          }
          if (flightSession != null) {
            for (int i = 0; i < flightSession.size(); i++) {
              if (flightSession.get(i) != null) {
                flightSession.get(i).close();
              }
            }
          }
        } catch (IoTDBConnectionException | StatementExecutionException e) {
          throw new RuntimeException(e);
        }
        flightSqlList.clear();
        flightDataIterator.clear();
        flightDataSet.clear();
        flightSession.clear();
        rulelCandidator.clear();
        timeList.clear();
        valueList.clear();
        cacheLines.clear();
      }
    }

    @Override
    public void beforeDestroy() {
      try {
        flightInfoDataSet.close();
        flightInfoSession.close();
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public boolean isFinish() {
      return finish;
    }
  }

  private String[] getTableNameList(String sourceList) {
    return sourceList.split(",");
  }

  private long parseTimeToNanos(String timeStr) {
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
    LocalDateTime dateTime = LocalDateTime.parse(timeStr, formatter);
    return dateTime.toEpochSecond(java.time.ZoneOffset.UTC) * 1000000000L;
  }

  public static class CacheLine {
    private long t1 = Long.MIN_VALUE;
    private long t2 = -1;
    private float value1;
    private float value2;
    private boolean finish = true;
    private Iterator<Long> timeIterator = null;
    private Iterator<Float> valueIterator = null;

    public CacheLine(Iterator<Long> timeIterator, Iterator<Float> valueIterator) {
      this.timeIterator = timeIterator;
      this.valueIterator = valueIterator;

    }

    // check 一下
    public void next() throws IoTDBConnectionException, StatementExecutionException {
      if (t1 == Long.MIN_VALUE) {
        finish = timeIterator.hasNext();
        t1 = timeIterator.next();
        value1 = valueIterator.next();
        finish = timeIterator.hasNext();
        if (finish) {
          t2 = timeIterator.next();
          value2 = valueIterator.next();
        }
      }
      finish = timeIterator.hasNext();
      t1 = t2;
      value1 = value2;
      if (finish) {
        t2 = timeIterator.next();
        value2 = valueIterator.next();
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

    public void reset() {
      t1 = Long.MIN_VALUE;
      t2 = -1;
      finish = true;
    }
  }
}
