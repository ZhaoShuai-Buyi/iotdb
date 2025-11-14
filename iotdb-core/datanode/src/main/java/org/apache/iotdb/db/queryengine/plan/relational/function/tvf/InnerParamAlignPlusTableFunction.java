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
import java.util.*;

/**
 * register method: sql:select * from
 * inner_param_align('alt_gps,alt_std_c,elevl,ff1,fqty,gw,latpc,lonpc,relevibpos,vrtg,vrtg2','default','previous','B-2028','2023-09-01T00:00:00','2023-09-30T23:59:59')
 *
 * reject session!
 *
 */
public class InnerParamAlignPlusTableFunction implements TableFunction {

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
    // 查询算子


    // 最高频索引
    private int sIndex = -1;
    private final String alignColumn = null; // 当前默认对齐列为最高频列，只需维护 sIndex 索引即可

    // 拼接 sql 以及所需参数
    private String[] tableNameList = null;
    private int size = 0;
    // private List<String> flightSqlList = null;

    // 时间间隔记录 & time 和 value 列缓存
    private List<ArrayList<Long>> timeList = null;
    private List<ArrayList<Float>> valueList = null;
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

    // 攒批相关参数
    private int columnBuilder_batch = 3000;
    private int columnBuilder_index = 0;

    // 其他缓存对象
    private final DateTimeFormatter formatter =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    private long pro_t1 = 0;
    private long pro_t2 = 0;
    private long append_t1 = 0;
    private long append_t2 = 0;

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

      this.timeList = new ArrayList<>(size);
      this.valueList = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        this.timeList.add(new ArrayList<>(columnBuilder_batch));
        this.valueList.add(new ArrayList<>(columnBuilder_batch));
      }

      this.tempResult = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        this.tempResult.add(new ArrayList<>(columnBuilder_batch));
      }
      this.tempTimestamp = new ArrayList<>(columnBuilder_batch);

      this.cacheLines = new ArrayList<CacheLine>(size);

      // address 为节点 ip 性能更好，取自配置文件中的 dn_rpc_address
      TEndPoint endPoint = IoTDBDescriptor.getInstance().getConfig().getAddressAndPort();
      address = endPoint.ip + ":" + endPoint.port;
    }

    @Override
    public void beforeStart() {
      try {
        String getAircraftMetadata = "select \"flight/datetime/startrecordingdatetime\", \"flight/datetime/endrecordingdatetime\" from flight_metadata where \"aircraft/tail\" = '"
                + aircraft
                + "' and \"flight/datetime/startrecordingdatetime\" >= '"
                + startTime
                + "' and \"flight/datetime/endrecordingdatetime\" <= '"
                + endTime
                + "'";
        // 算子初始化


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

          append_t2 = System.currentTimeMillis();
          pro_t2 = System.currentTimeMillis();
          System.out.println("per aircraft append cost: " + (append_t2 - append_t1) + "ms");
          System.out.println("per aircraft cost: " + (pro_t2 - pro_t1) + "ms");
        }

        // 判断是否还有航班
        if (!flightInfoDataIterator.next()) {
          finish = true;
          return;
        }
        pro_t1 = System.currentTimeMillis();

        long current_t1 = System.currentTimeMillis();
        // 当前航班所有迭代器
        String currentFlightStartTime = flightInfoDataIterator.getString(1);
        currentFlightStartTime =
            currentFlightStartTime.substring(0, currentFlightStartTime.length() - 1);
        String currentFlightEndTime = flightInfoDataIterator.getString(2);
        currentFlightEndTime = currentFlightEndTime.substring(0, currentFlightEndTime.length() - 1);
        long standardTime = parseTimeToNanos(currentFlightStartTime, formatter);
        long endding = parseTimeToNanos(currentFlightEndTime, formatter);
        for (int i = 0; i < size; i++) {
          String flightSQL =
              "select time, value from "
                  + tableNameList[i]
                  + " where \"aircraft/tail\" = '"
                  + aircraft
                  + "' and time >= "
                  + standardTime
                  + " and time <= "
                  + endding;
          SessionDataSet sessionDataSet = flightSession.get(i).executeQueryStatement(flightSQL);
          flightDataSet.add(sessionDataSet);
          flightDataIterator.add(sessionDataSet.iterator());
        }
        long current_t2 = System.currentTimeMillis();
        System.out.println("per aircraft current cost: " + (current_t2 - current_t1) + "ms");

        long init_t1 = System.currentTimeMillis();
        // 初始化 time 和 value 列，找并计算基准列
        long ruleTime = Long.MAX_VALUE;
        for (int i = 0; i < size; i++) {
          SessionDataSet.DataIterator dataIterator = flightDataIterator.get(i);
          ArrayList<Long> colTimeList = timeList.get(i);
          ArrayList<Float> colValueList = valueList.get(i);
          long last = -1;
          while (dataIterator.next()) {
            long cur = dataIterator.getLong("time");
            float val = dataIterator.getFloat("value");
            colTimeList.add(cur);
            colValueList.add(val);
            if (last != -1) {
              long diff = Math.abs(cur - last);
              if (diff < ruleTime) {
                ruleTime = diff;
              }
            }
            last = cur;
          }
        }
        long init_t2 = System.currentTimeMillis();
        System.out.println("per aircraft init cost: " + (init_t2 - init_t1) + "ms");

        long cache_t1 = System.currentTimeMillis();
        // 直接从开始时间按照 ruleTime 递增构造对齐结果集，其中，值写入中间对齐结果集 tempResult ，时间写入 tempTimestamp
        for (int i = 0; i < size; i++) {
          CacheLine cacheLine =
              new CacheLine(timeList.get(i).iterator(), valueList.get(i).iterator());
          cacheLines.add(cacheLine);
          // 初始化第一个数据点
          if (cacheLine.hasNext()) {
            cacheLine.next();
          }
        }
        long cache_t2 = System.currentTimeMillis();
        System.out.println("per aircraft cache cost: " + (cache_t2 - cache_t1) + "ms");

        // 对齐。补 null
        long dq_t1 = System.currentTimeMillis();
        while (standardTime <= endding) {
          tempTimestamp.add(standardTime);
          standardTime += ruleTime;
        }
        maxLine = tempTimestamp.size();
        long halfRuleTime = ruleTime >> 1;
        Iterator<Long> timeIterator;
        for (int i = 0; i < size; i++) {
          CacheLine currentCacheLine = cacheLines.get(i);
          List<Float> currentTempResult = tempResult.get(i);
          timeIterator = tempTimestamp.iterator();
          // todo: currentTempResult 扩容问题
          while (timeIterator.hasNext()) {
            long currentTime = timeIterator.next();
            if (currentCacheLine.hasNext()) {
              long firstDiff = Math.abs(currentCacheLine.getT1() - currentTime);
              long secondDiff =
                  currentCacheLine.hasNext()
                      ? Math.abs(currentCacheLine.getT2() - currentTime)
                      : Long.MAX_VALUE;
              if (firstDiff <= secondDiff && firstDiff <= halfRuleTime) {
                currentTempResult.add(currentCacheLine.getValue1());
                currentCacheLine.next();
              } else {
                currentTempResult.add(null);
              }
            } else {
              currentTempResult.add(currentCacheLine.getValue1());
            }
          }
        }
        long dq_t2 = System.currentTimeMillis();
        System.out.println("per aircraft dq cost: " + (dq_t2 - dq_t1) + "ms");

        // fill 全部 null 值
        long fill_t1 = System.currentTimeMillis();
        switch (fill) {
          case "previous":
            for (int i = 0; i < size; i++) {
              List<Float> currentResult = tempResult.get(i);
              float lastValue = 0.0f;
              for (int j = 0; j < maxLine; j++) {
                Float value = currentResult.get(j);
                if (value != null) {
                  lastValue = value;
                } else {
                  currentResult.set(j, lastValue);
                }
              }
            }
            break;
          case "next":
            for (int i = 0; i < size; i++) {
              List<Float> currentResult = tempResult.get(i);
              float lastValue = 0.0f;
              for (int j = maxLine - 1; j >= 0; j--) {
                Float value = currentResult.get(j);
                if (value != null) {
                  lastValue = value;
                } else {
                  currentResult.set(j, lastValue);
                }
              }
            }
            break;
        }
        long fill_t2 = System.currentTimeMillis();
        System.out.println("per aircraft fill cost: " + (fill_t2 - fill_t1) + "ms");

        append_t1 = System.currentTimeMillis();
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
        flightDataIterator.clear();
        flightDataSet.clear();
        for (int i = 0; i < size; i++) {
          timeList.get(i).clear();
          valueList.get(i).clear();
        }
        cacheLines.clear();
      }
    }

    @Override
    public void beforeDestroy() {
      try {
        // 算子的关闭处理？
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

  private long parseTimeToNanos(String timeStr, DateTimeFormatter formatter) {
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
