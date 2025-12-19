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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

/** high_user_anomaly_detect('electric','2025-08-01','1.5', '1', 'nx') 高压用户数据一致性分析校验 */
public class HighUserAnomalyDetectTableFunction implements TableFunction {

  private final String TABLE = "table";
  private final String DATE = "date";
  private final String K = "k";
  private final String BM = "bm";
  private final String DB = "db";

  @Override
  public List<ParameterSpecification> getArgumentsSpecifications() {
    return Arrays.asList(
        ScalarParameterSpecification.builder().name(TABLE).type(Type.STRING).build(),
        ScalarParameterSpecification.builder().name(DATE).type(Type.STRING).build(),
        ScalarParameterSpecification.builder()
            .name(K)
            .type(Type.STRING)
            .defaultValue("1.5")
            .build(),
        ScalarParameterSpecification.builder()
            .name(BM)
            .type(Type.STRING)
            .defaultValue("1")
            .build(),
    ScalarParameterSpecification.builder()
            .name(DB)
            .type(Type.STRING)
            .defaultValue("nxgw")
            .build());
  }

  @Override
  public TableFunctionAnalysis analyze(Map<String, Argument> arguments) throws UDFException {
    DescribedSchema schema =
        DescribedSchema.builder()
            .addField("id", Type.STRING)
            .addField("is_healthy", Type.BOOLEAN)
            .addField("reason", Type.STRING)
            .addField("window_start", Type.TIMESTAMP)
            .addField("window_end", Type.TIMESTAMP)
            .build();
    ScalarArgument bm = (ScalarArgument) arguments.get(BM);
    if (!"1".equals(bm.getValue()) && !"2".equals(bm.getValue())) {
      throw new UDFArgumentNotValidException("bm support '1' or '2'");
    }
    MapTableFunctionHandle handle =
        new MapTableFunctionHandle.Builder()
            .addProperty(TABLE, ((ScalarArgument) arguments.get(TABLE)).getValue())
            .addProperty(DATE, ((ScalarArgument) arguments.get(DATE)).getValue())
            .addProperty(K, ((ScalarArgument) arguments.get(K)).getValue())
            .addProperty(BM, ((ScalarArgument) arguments.get(BM)).getValue())
                .addProperty(DB, ((ScalarArgument) arguments.get(DB)).getValue())
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
        return new HighUserAnomalyDetectProcessor(
            (String) handle.getProperty(TABLE),
            (String) handle.getProperty(DATE),
            (String) handle.getProperty(K),
            (String) handle.getProperty(BM),
                (String) handle.getProperty(DB));
      }
    };
  }

  private class HighUserAnomalyDetectProcessor implements TableFunctionLeafProcessor {
    private String tableName;
    private String date;
    private String minusDate;
    private long minusDateStartTimestamp = 0L;
    private long minusDateEndTimestamp = 0L;
    private Float k;
    private Map<String, Float> allDocumentMap = new HashMap<>();
    private String bm;
    private String db;
    private final int line = 96;
    private boolean isFinish = false;
    private ITableSession session1;
    private ITableSession session2;
    private ITableSession session3;
    private SessionDataSet sessionDataSet1;
    private SessionDataSet sessionDataSet2;
    private SessionDataSet sessionDataSet3;

    private final Binary DetectNullException = new Binary("空值异常".getBytes());
    private final Binary DetectNegativeException = new Binary("负值异常".getBytes());
    private final Binary DetectSameTimePointDataException = new Binary("同时间点数据异常".getBytes());
    private final Binary DetectYestAllException = new Binary("昨日全部数据异常".getBytes());

    HighUserAnomalyDetectProcessor(String tableName, String date, String k, String bm, String db) {
      this.tableName = tableName;
      this.date = date;
      this.k = Float.parseFloat(k);
      this.bm = "f" + bm;
      this.db = db;
    }

    @Override
    public void beforeStart() {
      TEndPoint endPoint = IoTDBDescriptor.getInstance().getConfig().getAddressAndPort();
      String address = endPoint.ip + ":" + endPoint.port;
      minusDate = (LocalDate.parse(date)).minusDays(1).toString();
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
      ZoneId beijingZone = ZoneId.of("Asia/Shanghai");
      minusDateStartTimestamp = LocalDateTime.parse(minusDate + "T00:00:00", formatter).atZone(beijingZone).toInstant().toEpochMilli();
      minusDateEndTimestamp = LocalDateTime.parse(minusDate + "T23:45:00", formatter).atZone(beijingZone).toInstant().toEpochMilli();
      String sql1 = getGapfillSQL(bm, tableName, date);
      String sql2 = getGapfillSQL(bm, tableName, minusDate);
      String sql3 = "select id, rl from document";
      try {
        // query D
        session1 =
            new TableSessionBuilder()
                .nodeUrls(Collections.singletonList(address))
                .username("root")
                .password("root")
                .database(db)
                .build();
        // query D-1
        session2 =
            new TableSessionBuilder()
                .nodeUrls(Collections.singletonList(address))
                .username("root")
                .password("root")
                .database(db)
                .build();
        // query document
        session3 =
            new TableSessionBuilder()
                .nodeUrls(Collections.singletonList(address))
                .username("root")
                .password("root")
                .database(db)
                .build();
        sessionDataSet1 = session1.executeQueryStatement(sql1);
        sessionDataSet2 = session2.executeQueryStatement(sql2);
        sessionDataSet3 = session3.executeQueryStatement(sql3);
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        System.out.println(e.getMessage());
      }
    }

    @Override
    public void process(List<ColumnBuilder> columnBuilders) {
      try {
        long stand =
            LocalDateTime.parse(date + "T00:00:00").toInstant(ZoneOffset.ofHours(8)).toEpochMilli();
        SessionDataSet.DataIterator iterator1 = sessionDataSet1.iterator();
        SessionDataSet.DataIterator iterator2 = sessionDataSet2.iterator();
        SessionDataSet.DataIterator iterator3 = sessionDataSet3.iterator();
        while (iterator3.next()) {
          allDocumentMap.put(iterator3.getString(1), k * Float.parseFloat(iterator3.getString(2)));
        }
        Float rl = null;
        String id1 = null;
        String id2 = null;
        Binary id1Binary = null;
        Float cacheValue = 0.0F;
        boolean hasNext1 = iterator1.next();
        boolean hasNext2 = iterator2.next();
        if (hasNext1) {
          id1 = iterator1.getString(1);
          id1Binary = new Binary(id1.getBytes());
        }
        int count = 0;

        while (hasNext1 || hasNext2) {
          if (hasNext1) {
            id1 = iterator1.getString(1);
            if (count >= line) {
              count = 0;
              id1Binary = new Binary(id1.getBytes());
              cacheValue = 0.0f;
            }
          }
          if (hasNext2) {
            id2 = iterator2.getString(1);
          }
          long compareResult = 0;
          if (hasNext1 && hasNext2) {
            compareResult = compareId(id1, id2);
          } else if (hasNext1) {
            compareResult = -1;
          } else {
            compareResult = 1;
          }
          if (compareResult < 0) {
            // iterator1 正常研判当天的异常情况
            long currentTime = iterator1.getLong(2);
            if (iterator1.isNull(3)) {
              columnBuilders.get(0).writeBinary(id1Binary);
              columnBuilders.get(1).writeBoolean(false);
              columnBuilders.get(2).writeBinary(DetectNullException);
              columnBuilders.get(3).writeLong(currentTime);
              columnBuilders.get(4).writeLong(currentTime);
            } else {
              Float currentValue = iterator1.getFloat(3);
              if (currentValue - cacheValue < 0) {
                columnBuilders.get(0).writeBinary(id1Binary);
                columnBuilders.get(1).writeBoolean(false);
                columnBuilders.get(2).writeBinary(DetectNegativeException);
                columnBuilders.get(3).writeLong(currentTime);
                columnBuilders.get(4).writeLong(currentTime);
              }
              cacheValue = currentValue;
            }
            count++;
            hasNext1 = iterator1.next();
          } else if (compareResult > 0) {
            // iterator2 直接遍历一组数据
            for (int i = 0; i < line; i++) {
              hasNext2 = iterator2.next();
            }
          } else {
            // 正常执行 pn_id 与 time 相同时的异常识别
            try {
              rl = allDocumentMap.get(id1);
            } catch (Exception e) {
              rl = null;
            }
            long currentTime = iterator1.getLong(2);
            float currentValue1 = iterator1.getFloat(3);
            float currentValue2 = iterator2.getFloat(3);
            if (rl != null && currentValue1 - currentValue2 >= rl) {
              columnBuilders.get(0).writeBinary(id1Binary);
              columnBuilders.get(1).writeBoolean(false);
              columnBuilders.get(2).writeBinary(DetectYestAllException);
              columnBuilders.get(3).writeLong(minusDateStartTimestamp);
              columnBuilders.get(4).writeLong(minusDateEndTimestamp);
            }
            if (iterator1.isNull(3)) {
              columnBuilders.get(0).writeBinary(id1Binary);
              columnBuilders.get(1).writeBoolean(false);
              columnBuilders.get(2).writeBinary(DetectNullException);
              columnBuilders.get(3).writeLong(currentTime);
              columnBuilders.get(4).writeLong(currentTime);
            } else {
              if (currentValue1 - currentValue2 < 0) {
                columnBuilders.get(0).writeBinary(id1Binary);
                columnBuilders.get(1).writeBoolean(false);
                columnBuilders.get(2).writeBinary(DetectSameTimePointDataException);
                columnBuilders.get(3).writeLong(currentTime);
                columnBuilders.get(4).writeLong(currentTime);
              }
              if (currentValue1 - cacheValue < 0) {
                columnBuilders.get(0).writeBinary(id1Binary);
                columnBuilders.get(1).writeBoolean(false);
                columnBuilders.get(2).writeBinary(DetectNegativeException);
                columnBuilders.get(3).writeLong(currentTime);
                columnBuilders.get(4).writeLong(currentTime);
              }
              cacheValue = currentValue1;
            }
            hasNext1 = iterator1.next();
            hasNext2 = iterator2.next();
            count++;
          }
        }
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        System.out.println(e.getMessage());
      } finally {
        isFinish = true;
      }
    }

    @Override
    public boolean isFinish() {
      return isFinish;
    }

    @Override
    public void beforeDestroy() {
      try {
        if (sessionDataSet1 != null) {
          sessionDataSet1.close();
        }
        if (sessionDataSet2 != null) {
          sessionDataSet2.close();
        }
        if (sessionDataSet3 != null) {
          sessionDataSet3.close();
        }
        if (session1 != null) {
          session1.close();
        }
        if (session2 != null) {
          session2.close();
        }
        if (session3 != null) {
          session3.close();
        }
      } catch (Exception e) {
        System.out.println(e.getMessage());
      }
    }
  }

  public String getGapfillSQL(String bm, String tableName, String date) {
    return "select pn_id, date_bin_gapfill(15m, time), last("
            + bm
            + ") from "
            + tableName
            + " where time >= "
            + date
            + "T00:00:00 and time < "
            + date
            + "T23:59:59 group by 1,2";
  }

  public long compareId(String id1, String id2) {
    long num1 = Long.parseLong(id1);
    long num2 = Long.parseLong(id2);
    return Long.compare(num1, num2);
  }
}
