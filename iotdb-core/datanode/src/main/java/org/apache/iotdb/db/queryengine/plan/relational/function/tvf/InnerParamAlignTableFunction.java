package org.apache.iotdb.db.queryengine.plan.relational.function.tvf;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
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
import org.apache.tsfile.utils.Binary;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * register method: sql:select * from
 * inner_param_align('alt_gps,alt_std_c,elevl,ff1,fqty,gw,latpc,lonpc,relevibpos,vrtg,vrtg2','default','previous','B-2028','2023-09-01T00:00:00','2023-09-30T23:59:59')
 */
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
    if (!alignValidate)
      throw new UDFArgumentNotValidException("alignColumn support default or tableName");
    if (!"previous".equals(fill.getValue()) && !"next".equals(fill.getValue())) {
      throw new UDFArgumentNotValidException("fill support previous or next");
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
        MapTableFunctionHandle handle = (MapTableFunctionHandle) tableFunctionHandle;
        return new ParamAlignProcessor(
            (String) handle.getProperty(TABLE_LIST),
            (String) handle.getProperty(ALIGN_COLUMN),
            (String) handle.getProperty(FILL),
            (String) handle.getProperty(AIRCRAFT),
            (String) handle.getProperty(START_TIME),
            (String) handle.getProperty(END_TIME));
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
    private List<Iterator<Long>> timeIterators = null;
    private List<Iterator<Float>> valueIterators = null;
    private List<Long> rulelCandidator = null;
    private int maxLine = 0;

    // 单航班中间对齐结果集
    private List<List<Float>> tempResult = null;
    private List<Long> tempTimestamp = null;

    // 每一个实际列的两行 time 的迭代
    private List<CacheLine> cacheLines = null;

    // 整体结束条件
    private boolean finish = false;

    // 航班总信息
    private String fill = null;
    private String aircraft = null;
    private Binary aircraftOnly = null;
    private String startTime = null;
    private String endTime = null;

    // 本机地址
    String address = null;

    // fill 攒批相关参数
    private int columnBuilder_batch = 3000;
    private int columnBuilder_index = 0;
    private float previous_lastValue = 0.0f;
    private int next_nullCount = 0;
    private float next_cacheValue = 0.0f;

    private long pro_t1 = 0;
    private long pro_t2 = 0;

    ParamAlignProcessor(
        String tableList,
        String alignColumn,
        String fill,
        String aircraft,
        String startTime,
        String endTime) {
      this.fill = fill;
      this.aircraft = aircraft;
      this.aircraftOnly = new Binary(aircraft.getBytes());
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
      for (int i = 0; i < size; i++) {
        this.timeList.add(new ArrayList<>(columnBuilder_batch));
        this.valueList.add(new ArrayList<>(columnBuilder_batch));
      }
      timeIterators = new ArrayList<>(size);
      valueIterators = new ArrayList<>(size);

      this.rulelCandidator = new ArrayList<>(size);

      this.tempResult = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        this.tempResult.add(new ArrayList<>(columnBuilder_batch));
      }
      this.tempTimestamp = new ArrayList<>(columnBuilder_batch);

      this.cacheLines = new ArrayList<CacheLine>(size);

      TEndPoint endPoint = IoTDBDescriptor.getInstance().getConfig().getAddressAndPort();
      address = endPoint.ip + ":" + endPoint.port;
    }

    @Override
    public void beforeStart() {
      try {
        long t1 = System.currentTimeMillis();
        // 获取 metadata 迭代器
        flightInfoSession =
            new TableSessionBuilder()
                .nodeUrls(Collections.singletonList(address))
                .username("root")
                .password("root")
                .database("b777")
                .build();
        flightInfoDataSet =
            flightInfoSession.executeQueryStatement(
                "select * from flight_metadata where \"aircraft/tail\" = '"
                    + aircraft
                    + "' and \"flight/datetime/startrecordingdatetime\" >= '"
                    + startTime
                    + "' and \"flight/datetime/endrecordingdatetime\" <= '"
                    + endTime
                    + "'"); // 后续可选加入地点过滤条件
        flightInfoDataIterator = flightInfoDataSet.iterator();

        // 用同一批 session
        for (int i = 0; i < size; i++) {
          flightSession.add(
              new TableSessionBuilder()
                  .nodeUrls(Collections.singletonList(address))
                  .username("root")
                  .password("root")
                  .database("b777")
                  .build());
        }
        long t2 = System.currentTimeMillis();
        System.out.println("before start cost: " + (t2 - t1) + "ms");
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    // 每个 process 提交的契机都是完成一个航班的处理，需要处理资源获取和释放 modify:提交契机为一个航班内的攒批处理
    @Override
    public void process(List<ColumnBuilder> columnBuilders) {
      try {
        // 初始化查询语句，遍历并缓存所有的 time 和 value

        // 攒批判断，若前一个航班未处理完则继续处理
        if (columnBuilder_index < maxLine) {
          int temp_columnBuilder_index = columnBuilder_index;
          for (int i = 0; i < size; i++) {
            columnBuilder_index = temp_columnBuilder_index;
            List<Float> currentResult = tempResult.get(i);
            ColumnBuilder currentColumnBuilder = columnBuilders.get(i + 2);
            for (int j = 0; j < columnBuilder_batch; j++) {
              if (columnBuilder_index < maxLine) {
                currentColumnBuilder.writeFloat(currentResult.get(columnBuilder_index));
                columnBuilder_index++;
              } else {
                break;
              }
            }
          }
          columnBuilder_index = temp_columnBuilder_index;
          for (int j = 0; j < columnBuilder_batch; j++) {
            if (columnBuilder_index < maxLine) {
              columnBuilders.get(0).writeLong(tempTimestamp.get(columnBuilder_index));
              columnBuilders.get(1).writeBinary(aircraftOnly);
              columnBuilder_index++;
            } else {
              break;
            }
          }
          return;
        } else {
          for (int i = 0; i < size; i++) {
            tempResult.get(i).clear();
          }
          tempTimestamp.clear();
          columnBuilder_index = 0;

          previous_lastValue = 0.0f;
          next_nullCount = 0;
          next_cacheValue = 0.0f;

          pro_t2 = System.currentTimeMillis();
          System.out.println("per aircraft cost: " + (pro_t2 - pro_t1) + "ms");
        }

        // 判断是否还有航班
        if (!flightInfoDataIterator.next()) {
          finish = true;
          return;
        }
        pro_t1 = System.currentTimeMillis();

        for (int i = 0; i < size; i++) {
          rulelCandidator.add(Long.MAX_VALUE);
        }

        // 当前航班所有迭代器
        String currentFlightStartTime =
            flightInfoDataIterator.getString("flight/datetime/startrecordingdatetime");
        if (currentFlightStartTime.endsWith("Z")) {
          currentFlightStartTime =
              currentFlightStartTime.substring(0, currentFlightStartTime.length() - 1);
        }
        String currentFlightEndTime =
            flightInfoDataIterator.getString("flight/datetime/endrecordingdatetime");
        if (currentFlightEndTime.endsWith("Z")) {
          currentFlightEndTime =
              currentFlightEndTime.substring(0, currentFlightEndTime.length() - 1);
        }
        long standardTime = parseTimeToNanos(currentFlightStartTime);
        long endding = parseTimeToNanos(currentFlightEndTime);
        for (int i = 0; i < size; i++) {
          flightSqlList.add(
              "select time, value from "
                  + tableNameList[i]
                  + " where \"aircraft/tail\" = '"
                  + aircraft
                  + "' and time >= "
                  + standardTime
                  + " and time <= "
                  + endding);
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
            if (last != -1)
              rulelCandidator.set(i, Math.min(Math.abs(cur - last), rulelCandidator.get(i)));
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

        // 直接从开始时间按照 ruleTime 递增构造对齐结果集，其中，值写入中间对齐结果集 tempResult ，时间写入 tempTimestamp
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

        // 对齐。补 null
        while (standardTime <= endding) {
          tempTimestamp.add(standardTime);
          for (int i = 0; i < size; i++) {
            if (cacheLines.get(i).hasNext()) {
              long firstDiff = Math.abs(cacheLines.get(i).getT1() - standardTime);
              long secondDiff =
                  cacheLines.get(i).hasNext()
                      ? Math.abs(cacheLines.get(i).getT2() - standardTime)
                      : Long.MAX_VALUE;
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
        maxLine = tempTimestamp.size();

        // fill 全部 null 值
        switch (fill) {
          case "previous":
            for (int i = 0; i < size; i++) {
              List<Float> currentResult = tempResult.get(i);
              for (int j = 0; j < maxLine; j++) {
                Float value = currentResult.get(j);
                if (value != null) {
                  previous_lastValue = value;
                } else {
                  currentResult.set(j, previous_lastValue);
                }
              }
            }
            break;
          case "next":
            for (int i = 0; i < size; i++) {
              List<Float> currentResult = tempResult.get(i);
              for (int j = 0; j < maxLine; j++) {
                Float value = currentResult.get(j);
                if (value != null) {
                  next_cacheValue = value;
                  while (next_nullCount > 0) {
                    currentResult.set(j - next_nullCount, value);
                    next_nullCount--;
                  }
                } else {
                  next_nullCount++;
                }
              }
              while (next_nullCount > 0) {
                currentResult.set(maxLine - next_nullCount, next_cacheValue);
                next_nullCount--;
              }
            }
            break;
        }

        // 第一次 append columnBuilder
        int temp_columnBuilder_index = columnBuilder_index;
        for (int i = 0; i < size; i++) {
          columnBuilder_index = temp_columnBuilder_index;
          List<Float> currentResult = tempResult.get(i);
          ColumnBuilder currentColumnBuilder = columnBuilders.get(i + 2);
          for (int j = 0; j < columnBuilder_batch; j++) {
            if (columnBuilder_index < maxLine) {
              currentColumnBuilder.writeFloat(currentResult.get(columnBuilder_index));
              columnBuilder_index++;
            } else {
              break;
            }
          }
        }
        columnBuilder_index = temp_columnBuilder_index;
        for (int j = 0; j < columnBuilder_batch; j++) {
          if (columnBuilder_index < maxLine) {
            columnBuilders.get(0).writeLong(tempTimestamp.get(columnBuilder_index));
            columnBuilders.get(1).writeBinary(aircraftOnly);
            columnBuilder_index++;
          } else {
            break;
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
        } catch (IoTDBConnectionException | StatementExecutionException e) {
          throw new RuntimeException(e);
        }
        flightSqlList.clear();
        flightDataIterator.clear();
        flightDataSet.clear();
        rulelCandidator.clear();
        for (int i = 0; i < size; i++) {
          timeList.get(i).clear();
          valueList.get(i).clear();
        }
        timeIterators.clear();
        valueIterators.clear();
        cacheLines.clear();
      }
    }

    @Override
    public void beforeDestroy() {
      try {
        if (flightSession != null) {
          for (int i = 0; i < flightSession.size(); i++) {
            if (flightSession.get(i) != null) {
              flightSession.get(i).close();
            }
          }
        }
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

  private void fillWithXX() {}

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
