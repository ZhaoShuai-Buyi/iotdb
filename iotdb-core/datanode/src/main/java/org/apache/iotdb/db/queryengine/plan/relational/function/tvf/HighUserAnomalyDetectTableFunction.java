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
import java.time.ZoneOffset;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** high_user_anomaly_detect('electric','2025-08-01',1.5, '1') 高压用户数据一致性分析校验 */
public class HighUserAnomalyDetectTableFunction implements TableFunction {

  private final String TABLE = "table";
  private final String DATE = "date";
  private final String K = "k";
  private final String BM = "bm";

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
            (String) handle.getProperty(BM));
      }
    };
  }

  private class HighUserAnomalyDetectProcessor implements TableFunctionLeafProcessor {
    private String tableName;
    private String date;
    private String minusDate;
    private Float standRl;
    private Map<String, Float> allDocumentMap = new HashMap<>();
    private String bm;
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

    HighUserAnomalyDetectProcessor(String tableName, String date, String k, String bm) {
      this.tableName = tableName;
      this.date = date;
      this.standRl = Float.parseFloat(k) * 24;
      this.bm = "f" + bm;
    }

    @Override
    public void beforeStart() {
      TEndPoint endPoint = IoTDBDescriptor.getInstance().getConfig().getAddressAndPort();
      String address = endPoint.ip + ":" + endPoint.port;
      minusDate = (LocalDate.parse(date)).minusDays(1).toString();
      String sql1 =
          "select time ,pn_id, "
              + bm
              + " from "
              + tableName
              + " where time >= "
              + date
              + "T00:00:00 and time < "
              + date
              + "T23:59:59 order by pn_id, time asc";
      String sql2 =
          "select time ,pn_id, "
              + bm
              + " from "
              + tableName
              + " where time >= "
              + minusDate
              + "T00:00:00 and time < "
              + minusDate
              + "T23:59:59 order by pn_id, time asc";
      String sql3 = "select id, rv, cali_cur from document";
      try {
        // query D
        session1 =
            new TableSessionBuilder()
                .nodeUrls(Collections.singletonList(address))
                .username("root")
                .password("root")
                .database("nxgw")
                .build();
        // query D-1
        session2 =
            new TableSessionBuilder()
                .nodeUrls(Collections.singletonList(address))
                .username("root")
                .password("root")
                .database("nxgw")
                .build();
        // query document
        session3 =
            new TableSessionBuilder()
                .nodeUrls(Collections.singletonList(address))
                .username("root")
                .password("root")
                .database("nxgw")
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
          Float rv = 100.0f; // todo: update how to get voltage
          Matcher m = Pattern.compile("[（(]\\s*(\\d+)\\s*[)）]").matcher(iterator3.getString(3));
          Float caliCur = Float.parseFloat(m.find() ? m.group(1) : "0.0");
          allDocumentMap.put(iterator3.getString(1), standRl * rv * caliCur);
        }
        Float rl = standRl;
        String id = "";
        Binary idBinary = null;
        Float cacheValue = 0.0f;
        while (iterator1.next() && iterator2.next()) {
          long currentTime = iterator1.getLong(1);
          Float currentValue1 = iterator1.getFloat(3);
          Float currentValue2 = iterator2.getFloat(3);
          // 判断为零点则更换设备，也就要更换用户容量，并且重置 cacheValue
          if (currentTime == stand) {
            id = iterator1.getString(2);
            idBinary = new Binary(id.getBytes());
            rl = allDocumentMap.get(id);
            cacheValue = 0.0f;
            if (rl != null && currentValue1 - currentValue2 >= rl) {
              columnBuilders.get(0).writeBinary(idBinary);
              columnBuilders.get(1).writeBoolean(false);
              columnBuilders.get(2).writeBinary(DetectYestAllException);
              columnBuilders.get(3).writeBinary(new Binary((minusDate + "T00:00:00").getBytes()));
              columnBuilders.get(4).writeBinary(new Binary((minusDate + "T23:45:00").getBytes()));
            }
          }
          if (iterator1.isNull(3)) {
            columnBuilders.get(0).writeBinary(idBinary);
            columnBuilders.get(1).writeBoolean(false);
            columnBuilders.get(2).writeBinary(DetectNullException);
            columnBuilders.get(3).writeLong(currentTime);
            columnBuilders.get(4).writeLong(currentTime);
            continue;
          }
          if (currentValue1 - currentValue2 < 0) {
            columnBuilders.get(0).writeBinary(idBinary);
            columnBuilders.get(1).writeBoolean(false);
            columnBuilders.get(2).writeBinary(DetectSameTimePointDataException);
            columnBuilders.get(3).writeLong(currentTime);
            columnBuilders.get(4).writeLong(currentTime);
          }
          if (currentValue1 - cacheValue < 0) {
            columnBuilders.get(0).writeBinary(idBinary);
            columnBuilders.get(1).writeBoolean(false);
            columnBuilders.get(2).writeBinary(DetectNegativeException);
            columnBuilders.get(3).writeLong(currentTime);
            columnBuilders.get(4).writeLong(currentTime);
          }
          cacheValue = currentValue1;
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
}
