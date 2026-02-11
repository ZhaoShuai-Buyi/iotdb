package org.apache.iotdb.db.queryengine.plan.relational.function.tvf;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.InternalClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.rpc.TSStatusCode;
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

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.Binary;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/** sql:xx */
public class PVFTableFunction implements TableFunction {

  private final String PV_TABLE = "pv_table";
  private final String ANT_TABLE = "ant_table";
  private final String QIYUN_TABLE = "qiyun_table";
  private final String BAGUAN_TABLE = "baguan_table";
  private final String COVERAGE_RADIUS = "coverage_radius";
  private final String AGGREGATION = "aggregation";
  private final String START_TIME = "start_time";
  private final String END_TIME = "end_time";
  private final String MAX_INVERTER_NUM = "max_inverter_num";
  // 调试用
  private final String BATCH = "batch";

  /** 15 分钟对应的毫秒数 */
  private static final long FIFTEEN_MIN_MS = 900_000;

  /** 输出气象字段数（与 schema 4..13 列一致） */
  private static final int WEATHER_FIELD_COUNT = 11;

  @Override
  public List<ParameterSpecification> getArgumentsSpecifications() {
    return Arrays.asList(
        ScalarParameterSpecification.builder()
            .name(PV_TABLE)
            .type(Type.STRING)
            .defaultValue("ps_iot.st_inverter_prod")
            .build(),
        ScalarParameterSpecification.builder()
            .name(ANT_TABLE)
            .type(Type.STRING)
            .defaultValue("ant_weather.ant_weather_info")
            .build(),
        ScalarParameterSpecification.builder()
            .name(QIYUN_TABLE)
            .type(Type.STRING)
            .defaultValue("qiyun_weather.qi_yun_history_weather")
            .build(),
        ScalarParameterSpecification.builder()
            .name(BAGUAN_TABLE)
            .type(Type.STRING)
            .defaultValue("baguan_weather.seven_days_history_weather")
            .build(),
        ScalarParameterSpecification.builder()
            .name(COVERAGE_RADIUS)
            .type(Type.INT64)
            .defaultValue(10L)
            .build(),
        ScalarParameterSpecification.builder()
            .name(AGGREGATION)
            .type(Type.STRING)
            .defaultValue("average")
            .build(),
        ScalarParameterSpecification.builder().name(START_TIME).type(Type.STRING).build(),
        ScalarParameterSpecification.builder().name(END_TIME).type(Type.STRING).build(),
        ScalarParameterSpecification.builder()
            .name(MAX_INVERTER_NUM)
            .type(Type.INT64)
            .defaultValue(10000L)
            .build(),
        ScalarParameterSpecification.builder()
            .name(BATCH)
            .type(Type.INT64)
            .defaultValue(10L)
            .build());
  }

  @Override
  public TableFunctionAnalysis analyze(Map<String, Argument> arguments) throws UDFException {
    // 结果集 schema
    DescribedSchema schema =
        DescribedSchema.builder()
            .addField("time", Type.TIMESTAMP)
            .addField("ps_id", Type.STRING)
            .addField("sn", Type.STRING)
            .addField("pac", Type.DOUBLE)
            .addField("tenmeterswindspeed", Type.DOUBLE)
            .addField("tenmeterswinddirection", Type.DOUBLE)
            .addField("eightymeterswindspeed", Type.DOUBLE)
            .addField("eightymeterswinddirection", Type.DOUBLE)
            .addField("onehundredandtwentymeterswindspeed", Type.DOUBLE)
            .addField("onehundredandtwentymeterswinddirection", Type.DOUBLE)
            .addField("totalcloudcover", Type.DOUBLE)
            .addField("surfacepressure", Type.DOUBLE)
            .addField("irradiance", Type.DOUBLE)
            .addField("scatteredradiation", Type.DOUBLE)
            .addField("directradiation", Type.DOUBLE)
            .build();
    // 校验
    String aggregation_check = (String) ((ScalarArgument) arguments.get(AGGREGATION)).getValue();
    if (!"average".equals(aggregation_check)
        && !"weighted_average".equals(aggregation_check)
        && !"slerp".equals(aggregation_check)) {
      throw new UDFArgumentNotValidException(
          "aggregation support average, weighted_average, slerp");
    }

    // 入参传递
    MapTableFunctionHandle handle =
        new MapTableFunctionHandle.Builder()
            .addProperty(PV_TABLE, ((ScalarArgument) arguments.get(PV_TABLE)).getValue())
            .addProperty(ANT_TABLE, ((ScalarArgument) arguments.get(ANT_TABLE)).getValue())
            .addProperty(QIYUN_TABLE, ((ScalarArgument) arguments.get(QIYUN_TABLE)).getValue())
            .addProperty(BAGUAN_TABLE, ((ScalarArgument) arguments.get(BAGUAN_TABLE)).getValue())
            .addProperty(
                COVERAGE_RADIUS, ((ScalarArgument) arguments.get(COVERAGE_RADIUS)).getValue())
            .addProperty(AGGREGATION, ((ScalarArgument) arguments.get(AGGREGATION)).getValue())
            .addProperty(START_TIME, ((ScalarArgument) arguments.get(START_TIME)).getValue())
            .addProperty(END_TIME, ((ScalarArgument) arguments.get(END_TIME)).getValue())
            .addProperty(
                MAX_INVERTER_NUM, ((ScalarArgument) arguments.get(MAX_INVERTER_NUM)).getValue())
            .addProperty(BATCH, ((ScalarArgument) arguments.get(BATCH)).getValue())
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
        return new PVFProcessor(
            (String) handle.getProperty(PV_TABLE),
            (String) handle.getProperty(ANT_TABLE),
            (String) handle.getProperty(QIYUN_TABLE),
            (String) handle.getProperty(BAGUAN_TABLE),
            (Long) handle.getProperty(COVERAGE_RADIUS),
            (String) handle.getProperty(AGGREGATION),
            (String) handle.getProperty(START_TIME),
            (String) handle.getProperty(END_TIME),
            (Long) handle.getProperty(MAX_INVERTER_NUM),
            (Long) handle.getProperty(BATCH));
      }
    };
  }

  /** sql summary:xx */
  private class PVFProcessor implements TableFunctionLeafProcessor {
    private boolean finish = false;

    private long batch = 10;
    private int index = 0;

    private String pvTable;
    private String antTable;
    private String qiyunTable;
    private String baguanTable;
    private long coverageRadius;
    private String aggregation;
    private String startTime;
    private String endTime;
    private long maxInverterNum;

    private IClientSession internalSession = null;
    private Coordinator coordinator = Coordinator.getInstance();
    private SessionManager sessionManager = SessionManager.getInstance();
    private Metadata metadata = LocalExecutionPlanner.getInstance().metadata;
    private SqlParser sqlParser = new SqlParser();

    // 位置信息
    private List<PsAndSn> inverterIds = new ArrayList<>(98304);
    private List<LatAndLon> weatherLocs = new ArrayList<>(32768);
    private Map<PsAndSn, List<LatAndLon>> locsMap = new HashMap<>(98304);

    // 时序数据
    private List<List<Double>> weathers = new ArrayList<>(10);

    /** 气象按站缓存：key = lat + "\\t" + lon + "\\t" + source，同一时间范围内同一站只查一次 */
    private final Map<String, List<double[]>> weatherCache = new HashMap<>();

    PVFProcessor(
        String pvTable,
        String antTable,
        String qiyunTable,
        String baguanTable,
        long coverageRadius,
        String aggregation,
        String startTime,
        String endTime,
        long maxInverterNum,
        long batch) {
      this.pvTable = pvTable;
      this.antTable = antTable;
      this.qiyunTable = qiyunTable;
      this.baguanTable = baguanTable;
      this.coverageRadius = coverageRadius;
      this.aggregation = aggregation;
      this.startTime = startTime;
      this.endTime = endTime;
      this.maxInverterNum = maxInverterNum;
      this.batch = batch;

      for (int i = 0; i < 10; i++) {
        List<Double> field = new ArrayList<>(98304);
        weathers.add(field);
      }
    }

    public void addWeatherLocs(String sql, String source) {
      long queryId =
          sessionManager.requestQueryId(
              internalSession, sessionManager.requestStatementId(internalSession));
      Throwable throwable = null;
      try {
        Statement statement =
            sqlParser.createStatement(sql, internalSession.getZoneId(), internalSession);
        ExecutionResult result =
            coordinator.executeForTableModel(
                statement,
                sqlParser,
                internalSession,
                queryId,
                sessionManager.getSessionInfoOfTableModel(internalSession),
                sql,
                metadata,
                3600000,
                false);
        if (result.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
            && result.status.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
          throw new IoTDBException(result.status.getMessage(), result.status.getCode());
        }
        IQueryExecution queryExecution = coordinator.getQueryExecution(queryId);
        while (queryExecution.hasNextResult()) {
          Optional<TsBlock> tsBlockOpt = queryExecution.getBatchResult();
          if (!tsBlockOpt.isPresent()) {
            continue;
          }
          TsBlock tsBlock = tsBlockOpt.get();
          if (tsBlock.isEmpty()) {
            continue;
          }
          Column lat = tsBlock.getColumn(0);
          Column lng = tsBlock.getColumn(1);
          int positionCount = tsBlock.getPositionCount();

          String addSource = "";

          if ("ant".equals(source)) {
            addSource = "ant";
          } else if ("qiyun".equals(source)) {
            addSource = "qiyun";
          } else if ("baguan".equals(source)) {
            addSource = "baguan";
          }

          for (int i = 0; i < positionCount; i++) {
            if (lat.isNull(i) || lng.isNull(i)) {
              continue;
            }
            weatherLocs.add(new LatAndLon(lat.getBinary(i), lng.getBinary(i), addSource));
          }
        }
      } catch (Throwable t) {
        throwable = t;
        throw new RuntimeException("Failed to execute query: " + sql, t);
      } finally {
        coordinator.cleanupQueryExecution(queryId, (Supplier<String>) null, throwable);
      }
    }

    public void addInverterIdsAndLocsMap(String sql) {
      long queryId =
          sessionManager.requestQueryId(
              internalSession, sessionManager.requestStatementId(internalSession));
      Throwable throwable = null;
      try {
        Statement statement =
            sqlParser.createStatement(sql, internalSession.getZoneId(), internalSession);
        ExecutionResult result =
            coordinator.executeForTableModel(
                statement,
                sqlParser,
                internalSession,
                queryId,
                sessionManager.getSessionInfoOfTableModel(internalSession),
                sql,
                metadata,
                3600000,
                false);
        if (result.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
            && result.status.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
          throw new IoTDBException(result.status.getMessage(), result.status.getCode());
        }
        IQueryExecution queryExecution = coordinator.getQueryExecution(queryId);
        while (queryExecution.hasNextResult()) {
          Optional<TsBlock> tsBlockOpt = queryExecution.getBatchResult();
          if (!tsBlockOpt.isPresent()) {
            continue;
          }
          TsBlock tsBlock = tsBlockOpt.get();
          if (tsBlock.isEmpty()) {
            continue;
          }
          Column psId = tsBlock.getColumn(0);
          Column sn = tsBlock.getColumn(1);
          Column lat = tsBlock.getColumn(2);
          Column lng = tsBlock.getColumn(3);

          int positionCount = tsBlock.getPositionCount();

          for (int i = 0; i < positionCount; i++) {
            if (psId.isNull(i) || sn.isNull(i) || lat.isNull(i) || lng.isNull(i)) {
              continue;
            }
            PsAndSn psAndSn = new PsAndSn(psId.getBinary(i), sn.getBinary(i));
            inverterIds.add(psAndSn);
            List<LatAndLon> locs = new ArrayList<>();
            for (LatAndLon singleWeatherStation : weatherLocs) {
              float distance =
                  haversineDistance(
                      Float.parseFloat(lat.getBinary(i).toString()),
                      Float.parseFloat(lng.getBinary(i).toString()),
                      Float.parseFloat(singleWeatherStation.lat.toString()),
                      Float.parseFloat(singleWeatherStation.lon.toString()));
              if (distance <= coverageRadius) {
                locs.add(singleWeatherStation);
              }
            }
            locsMap.put(psAndSn, locs);
          }
        }
      } catch (Throwable t) {
        throwable = t;
        throw new RuntimeException("Failed to execute query: " + sql, t);
      } finally {
        coordinator.cleanupQueryExecution(queryId, (Supplier<String>) null, throwable);
      }
    }

    public void queryInverterPac(String sql) {}

    /** 根据气象站位置与数据源构造气象查询 SQL（与 schema 字段一一对应） */
    private String buildWeatherSql(LatAndLon loc, long startMs, long endMs) {
      String latStr = loc.lat.toString();
      String lonStr = loc.lon.toString();
      if ("ant".equals(loc.source)) {
        return "select time, wind_speed_10m, wind_direction_10m, wind_speed_80m, wind_direction_80m, wind_speed_120m, wind_direction_120m, cloud_cover, surface_pressure, shortwave_radiation, diffuse_radiation, direct_radiation from "
            + antTable
            + " where lat = '"
            + latStr
            + "' and lng = '"
            + lonStr
            + "' and time >= "
            + startMs
            + " and time <= "
            + endMs;
      } else if ("qiyun".equals(loc.source)) {
        return "select time, tenmeterswindspeed, tenmeterswinddirection, eightymeterswindspeed, eightymeterswinddirection, onehundredandtwentymeterswindspeed, onehundredandtwentymeterswinddirection, totalcloudcover, surfacepressure, irradiance, scatteredradiation, directradiation from "
            + qiyunTable
            + " where latitude = '"
            + latStr
            + "' and longitude = '"
            + lonStr
            + "' and time >= "
            + startMs
            + " and time <= "
            + endMs;
      } else if ("baguan".equals(loc.source)) {
        return "select time, tenmeterswindspeed, tenmeterswinddirection, eightymeterswindspeed, eightymeterswinddirection, onehundredandtwentymeterswindspeed, onehundredandtwentymeterswinddirection, totalcloudcover, surfacepressure, totalradiation, scatteredradiation, directradiation from "
            + baguanTable
            + " where latitude = '"
            + latStr
            + "' and longitude = '"
            + lonStr
            + "' and time >= "
            + startMs
            + " and time <= "
            + endMs;
      } else {
        throw new IllegalArgumentException("unknown weather source: " + loc.source);
      }
    }

    /** 执行单站气象查询，按 15min 槽对齐填充；缺失槽填 Double.NaN。 返回 list 下标与 [startMs, endMs] 内 15min 槽顺序一致。 */
    private List<double[]> runWeatherQuery(String sql, String source, long startMs, long endMs) {
      int nSlots = (int) ((endMs - startMs) / FIFTEEN_MIN_MS) + 1;
      List<double[]> bySlot = new ArrayList<>(nSlots);
      for (int i = 0; i < nSlots; i++) {
        double[] row = new double[WEATHER_FIELD_COUNT];
        Arrays.fill(row, Double.NaN);
        bySlot.add(row);
      }
      long queryId =
          sessionManager.requestQueryId(
              internalSession, sessionManager.requestStatementId(internalSession));
      Throwable throwable = null;
      try {
        Statement statement =
            sqlParser.createStatement(sql, internalSession.getZoneId(), internalSession);
        ExecutionResult result =
            coordinator.executeForTableModel(
                statement,
                sqlParser,
                internalSession,
                queryId,
                sessionManager.getSessionInfoOfTableModel(internalSession),
                sql,
                metadata,
                3600000,
                false);
        if (result.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
            && result.status.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
          throw new IoTDBException(result.status.getMessage(), result.status.getCode());
        }
        IQueryExecution queryExecution = coordinator.getQueryExecution(queryId);
        while (queryExecution.hasNextResult()) {
          Optional<TsBlock> tsBlockOpt = queryExecution.getBatchResult();
          if (!tsBlockOpt.isPresent()) continue;
          TsBlock tsBlock = tsBlockOpt.get();
          if (tsBlock.isEmpty()) continue;
          Column timeCol = tsBlock.getColumn(0);
          int posCount = tsBlock.getPositionCount();
          for (int i = 0; i < posCount; i++) {
            if (timeCol.isNull(i)) continue;
            long t = timeCol.getLong(i);
            int slotIndex = (int) ((t - startMs) / FIFTEEN_MIN_MS);
            if (slotIndex < 0 || slotIndex >= nSlots) continue;
            double[] out = bySlot.get(slotIndex);
            out[0] = getDouble(tsBlock, 1, i);
            out[1] = getDouble(tsBlock, 2, i);
            out[2] = getDouble(tsBlock, 3, i);
            out[3] = getDouble(tsBlock, 4, i);
            out[4] = getDouble(tsBlock, 5, i);
            out[5] = getDouble(tsBlock, 6, i);
            out[6] = getDouble(tsBlock, 7, i);
            out[7] = getDouble(tsBlock, 8, i);
            out[8] = getDouble(tsBlock, 9, i);
            out[9] = getDouble(tsBlock, 10, i);
            out[10] = getDouble(tsBlock, 11, i);
          }
        }
      } catch (Throwable t) {
        throwable = t;
        throw new RuntimeException("Failed to execute weather query: " + sql, t);
      } finally {
        coordinator.cleanupQueryExecution(queryId, (Supplier<String>) null, throwable);
      }
      return bySlot;
    }

    private double getDouble(TsBlock block, int colIndex, int rowIndex) {
      Column c = block.getColumn(colIndex);
      return c.isNull(rowIndex) ? Double.NaN : c.getDouble(rowIndex);
    }

    /** 按 time 升序排序，同步交换 pac 列表，保证一一对应 */
    private void sortByTime(List<Long> times, List<Double> pacs) {
      int n = times.size();
      Integer[] idx = new Integer[n];
      for (int i = 0; i < n; i++) idx[i] = i;
      Arrays.sort(idx, Comparator.comparingLong(times::get));
      List<Long> t2 = new ArrayList<>(n);
      List<Double> p2 = new ArrayList<>(n);
      for (int i = 0; i < n; i++) {
        t2.add(times.get(idx[i]));
        p2.add(pacs.get(idx[i]));
      }
      times.clear();
      pacs.clear();
      times.addAll(t2);
      pacs.addAll(p2);
    }

    /**
     * 对设备关联的多个气象站，按 15min 槽聚合：同一物理量取平均后写入输出顺序。 locs 为空时返回全 NaN 的槽序列。同一气象站在同一时间范围内只查一次（使用
     * weatherCache）。
     */
    private List<double[]> getAveragedWeatherBy15MinSlots(
        List<LatAndLon> locs, long startMs, long endMs) {
      int nSlots = (int) ((endMs - startMs) / FIFTEEN_MIN_MS) + 1;
      double[][] sum = new double[nSlots][WEATHER_FIELD_COUNT];
      int[][] count = new int[nSlots][WEATHER_FIELD_COUNT];
      if (locs != null && !locs.isEmpty()) {
        for (LatAndLon loc : locs) {
          String cacheKey = loc.lat.toString() + "\t" + loc.lon.toString() + "\t" + loc.source;
          List<double[]> station = weatherCache.get(cacheKey);
          if (station == null) {
            String sql = buildWeatherSql(loc, startMs, endMs);
            station = runWeatherQuery(sql, loc.source, startMs, endMs);
            weatherCache.put(cacheKey, station);
          }
          for (int s = 0; s < nSlots; s++) {
            for (int f = 0; f < WEATHER_FIELD_COUNT; f++) {
              double v = station.get(s)[f];
              if (!Double.isNaN(v)) {
                sum[s][f] += v;
                count[s][f]++;
              }
            }
          }
        }
      }
      List<double[]> result = new ArrayList<>(nSlots);
      for (int s = 0; s < nSlots; s++) {
        double[] row = new double[WEATHER_FIELD_COUNT];
        for (int f = 0; f < WEATHER_FIELD_COUNT; f++) {
          row[f] = count[s][f] == 0 ? Double.NaN : (sum[s][f] / count[s][f]);
        }
        result.add(row);
      }
      return result;
    }

    @Override
    public void beforeStart() {
      try {
        internalSession = new InternalClientSession("query");
        sessionManager.registerSession(internalSession);
        sessionManager.supplySession(
            internalSession, -1, "root", ZoneId.systemDefault(), IoTDBConstant.ClientVersion.V_1_0);
        // 初始化位置信息
        addWeatherLocs(
            "select distinct lat, lng from "
                + antTable
                + " where lat is not null and lng is not null",
            "ant");
        addWeatherLocs(
            "select distinct latitude, longitude from "
                + qiyunTable
                + " where latitude is not null and longitude is not null",
            "qiyun");
        addWeatherLocs(
            "select distinct latitude, longitude from "
                + baguanTable
                + " where latitude is not null and longitude is not null",
            "baguan");
        addInverterIdsAndLocsMap(
            "select distinct ps_id, sn, lat, lng from "
                + pvTable
                + " where lat is not null and lng is not null");
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    /**
     * 攒够 batch 个设备的数据提交一次 process，全部处理完后修改 finish
     *
     * @param columnBuilders
     */
    @Override
    public void process(List<ColumnBuilder> columnBuilders) {
      long startMs = parseTime(startTime);
      long endMs = parseTime(endTime);
      int currentProcessCount = 0;
      for (int i = 0;
          i < batch && index < inverterIds.size();
          i++, currentProcessCount++, index++) {
        PsAndSn currentPsAndSn = inverterIds.get(index);
        List<LatAndLon> locs = locsMap.get(currentPsAndSn);
        List<double[]> deviceWeatherBySlot =
            getAveragedWeatherBy15MinSlots(
                locs != null ? locs : Collections.emptyList(), startMs, endMs);

        long queryId =
            sessionManager.requestQueryId(
                internalSession, sessionManager.requestStatementId(internalSession));
        String currentInverterSql =
            "select time, pac from "
                + pvTable
                + " where ps_id = '"
                + currentPsAndSn.ps_id.toString()
                + "' and sn = '"
                + currentPsAndSn.sn.toString()
                + "' and time >= "
                + startMs
                + " and time <= "
                + endMs;
        Throwable throwable = null;
        try {
          Statement statement =
              sqlParser.createStatement(
                  currentInverterSql, internalSession.getZoneId(), internalSession);
          ExecutionResult result =
              coordinator.executeForTableModel(
                  statement,
                  sqlParser,
                  internalSession,
                  queryId,
                  sessionManager.getSessionInfoOfTableModel(internalSession),
                  currentInverterSql,
                  metadata,
                  3600000,
                  false);
          if (result.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
              && result.status.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
            throw new IoTDBException(result.status.getMessage(), result.status.getCode());
          }
          IQueryExecution queryExecution = coordinator.getQueryExecution(queryId);
          List<Long> pacTimes = new ArrayList<>();
          List<Double> pacValues = new ArrayList<>();
          while (queryExecution.hasNextResult()) {
            Optional<TsBlock> tsBlockOpt = queryExecution.getBatchResult();
            if (!tsBlockOpt.isPresent()) continue;
            TsBlock tsBlock = tsBlockOpt.get();
            if (tsBlock.isEmpty()) continue;
            Column timeCol = tsBlock.getColumn(0);
            Column pacCol = tsBlock.getColumn(1);
            int len = tsBlock.getPositionCount();
            for (int p = 0; p < len; p++) {
              if (timeCol.isNull(p)) continue;
              pacTimes.add(timeCol.getLong(p));
              pacValues.add(pacCol.isNull(p) ? Double.NaN : pacCol.getDouble(p));
            }
          }
          sortByTime(pacTimes, pacValues);
          int pacPosition = 0;
          int pacSize = pacTimes.size();
          long currentTime = startMs;
          int slotIndex = 0;
          while (currentTime <= endMs) {
            columnBuilders.get(0).writeLong(currentTime);
            columnBuilders.get(1).writeBinary(currentPsAndSn.ps_id);
            columnBuilders.get(2).writeBinary(currentPsAndSn.sn);
            while (pacPosition < pacSize && pacTimes.get(pacPosition) < currentTime) {
              pacPosition++;
            }
            long inverterTime = pacPosition < pacSize ? pacTimes.get(pacPosition) : Long.MAX_VALUE;
            if (inverterTime > currentTime) {
              columnBuilders.get(3).appendNull();
            } else if (inverterTime == currentTime) {
              double pacVal = pacValues.get(pacPosition);
              if (Double.isNaN(pacVal)) {
                columnBuilders.get(3).appendNull();
              } else {
                columnBuilders.get(3).writeDouble(pacVal);
              }
              pacPosition++;
            } else {
              columnBuilders.get(3).appendNull();
            }
            double[] weather = deviceWeatherBySlot.get(slotIndex);
            for (int w = 0; w < WEATHER_FIELD_COUNT; w++) {
              if (Double.isNaN(weather[w])) {
                columnBuilders.get(4 + w).appendNull();
              } else {
                columnBuilders.get(4 + w).writeDouble(weather[w]);
              }
            }
            currentTime += FIFTEEN_MIN_MS;
            slotIndex++;
          }
        } catch (Throwable t) {
          throwable = t;
          throw new RuntimeException("Failed to execute query: " + currentInverterSql, t);
        } finally {
          coordinator.cleanupQueryExecution(queryId, (Supplier<String>) null, throwable);
        }
      }
      if (currentProcessCount < batch) {
        finish = true;
      }
    }

    @Override
    public void beforeDestroy() {
      try {
        sessionManager.closeSession(internalSession, coordinator::cleanupQueryExecution);
        sessionManager.removeCurrSession();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public boolean isFinish() {
      return finish;
    }
  }

  private static class PsAndSn {
    public Binary ps_id;
    public Binary sn;

    public PsAndSn(Binary ps_id, Binary sn) {
      this.ps_id = ps_id;
      this.sn = sn;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      PsAndSn psAndSn = (PsAndSn) o;
      return Objects.equals(ps_id, psAndSn.ps_id) && Objects.equals(sn, psAndSn.sn);
    }

    @Override
    public int hashCode() {
      return Objects.hash(ps_id, sn);
    }
  }

  private static class LatAndLon {
    public Binary lat;
    public Binary lon;
    public String source;

    public LatAndLon(Binary lat, Binary lon, String source) {
      this.lat = lat;
      this.lon = lon;
      this.source = source;
    }
  }

  /** 将形如 2022-01-01T00:00:00 的时间字符串解析为毫秒时间戳（epoch milli） */
  public long parseTime(String time) {
    // 使用 ISO_LOCAL_DATE_TIME 解析，例如 2022-01-01T00:00:00
    LocalDateTime localDateTime = LocalDateTime.parse(time, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    // 按系统默认时区转换为毫秒时间戳
    return localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
  }

  /**
   * 使用 Haversine 公式计算两点间球面距离（单位：千米）
   *
   * @param sourLat 源点纬度（度）
   * @param sourLon 源点经度（度）
   * @param tarLat 目标点纬度（度）
   * @param tarLon 目标点经度（度）
   * @return 距离（千米）
   */
  public float haversineDistance(float sourLat, float sourLon, float tarLat, float tarLon) {
    // 内部用 double 计算以减少累计误差
    double sourLatRadians = Math.toRadians(sourLat);
    double sourLonRadians = Math.toRadians(sourLon);
    double tarLatRadians = Math.toRadians(tarLat);
    double tarLonRadians = Math.toRadians(tarLon);

    // 计算差值
    double latDiff = tarLatRadians - sourLatRadians;
    double lonDiff = tarLonRadians - sourLonRadians;

    // haversine 公式
    double a =
        Math.sin(latDiff / 2) * Math.sin(latDiff / 2)
            + Math.cos(sourLatRadians)
                * Math.cos(tarLatRadians)
                * Math.sin(lonDiff / 2)
                * Math.sin(lonDiff / 2);
    double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

    // 地球平均半径（千米）
    double R = 6371.0;

    double distanceKm = R * c;
    return (float) distanceKm;
  }
}
