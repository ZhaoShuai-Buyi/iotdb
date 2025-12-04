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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** dim_integrity_rate('electric','2025-08-01','all' | 'single') 数据采集完整性分析 */
public class DimIntegrityRateTableFunction implements TableFunction {

  private final String TABLE = "table";
  private final String DATE = "date";
  private final String METHOD = "method";

  @Override
  public List<ParameterSpecification> getArgumentsSpecifications() {
    return Arrays.asList(
        ScalarParameterSpecification.builder().name(TABLE).type(Type.STRING).build(),
        ScalarParameterSpecification.builder().name(DATE).type(Type.STRING).build(),
        ScalarParameterSpecification.builder()
            .name(METHOD)
            .type(Type.STRING)
            .defaultValue("all")
            .build());
  }

  @Override
  public TableFunctionAnalysis analyze(Map<String, Argument> arguments) throws UDFException {
    DescribedSchema schema =
        DescribedSchema.builder()
            .addField("method", Type.STRING)
            .addField("completeness_rate", Type.FLOAT)
            .addField("entity", Type.STRING)
            .build();
    ScalarArgument method = (ScalarArgument) arguments.get(METHOD);
    if (!"all".equals(method.getValue()) && !"single".equals(method.getValue())) {
      throw new UDFArgumentNotValidException("method support all or single");
    }
    MapTableFunctionHandle handle =
        new MapTableFunctionHandle.Builder()
            .addProperty(TABLE, ((ScalarArgument) arguments.get(TABLE)).getValue())
            .addProperty(DATE, ((ScalarArgument) arguments.get(DATE)).getValue())
            .addProperty(METHOD, ((ScalarArgument) arguments.get(METHOD)).getValue())
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
        return new DimIntegrityRateProcessor(
            (String) handle.getProperty(TABLE),
            (String) handle.getProperty(DATE),
            (String) handle.getProperty(METHOD));
      }
    };
  }

  private class DimIntegrityRateProcessor implements TableFunctionLeafProcessor {
    private String method;
    private String tableName;
    private String date;
    private ITableSession session;
    private SessionDataSet sessionDataSet;
    private String sql;
    private boolean isFinish = false;

    DimIntegrityRateProcessor(String tableName, String date, String method) {
      this.method = method;
      this.tableName = tableName;
      this.date = date;
    }

    @Override
    public void beforeStart() {
      TEndPoint endPoint = IoTDBDescriptor.getInstance().getConfig().getAddressAndPort();
      String address = endPoint.ip + ":" + endPoint.port;
      // String afterDate = (LocalDate.parse(date)).plusDays(1).toString();
      sql =
          "select pn_id, count(f1) from "
              + tableName
              + " where time >= "
              + date
              + "T00:00:00 and time < "
              + date
              + "T23:59:59 group by pn_id";
      try {
        session =
            new TableSessionBuilder()
                .nodeUrls(Collections.singletonList(address))
                .username("root")
                .password("root")
                .database("nx")
                .build();
      } catch (IoTDBConnectionException e) {
        System.out.println(e.getMessage());
      }
    }

    @Override
    public void process(List<ColumnBuilder> columnBuilders) {
      if ("single".equals(method)) {
        processSingle(columnBuilders);
      } else {
        processAll(columnBuilders);
      }
      isFinish = true;
    }

    @Override
    public boolean isFinish() {
      return isFinish;
    }

    @Override
    public void beforeDestroy() {
      try {
        if (sessionDataSet != null) {
          sessionDataSet.close();
        }
        if (session != null) {
          session.close();
        }
      } catch (Exception e) {
        System.out.println(e.getMessage());
      }
    }

    private void processAll(List<ColumnBuilder> columnBuilders) {
      try {
        sessionDataSet = session.executeQueryStatement(sql);
        SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
        long countAll = 0;
        long countTarget = 0;
        while (iterator.next()) {
          countAll++;
          if (iterator.getInt(2) == 96) {
            countTarget++;
          }
        }
        float rate = countTarget / (countAll == 0 ? 1.0f : countAll);
        columnBuilders.get(0).writeBinary(new Binary(method.getBytes()));
        columnBuilders.get(1).writeFloat(rate);
        columnBuilders.get(2).writeBinary(new Binary(method.getBytes()));
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        System.out.println(e.getMessage());
      }
    }

    private void processSingle(List<ColumnBuilder> columnBuilders) {
      try {
        sessionDataSet = session.executeQueryStatement(sql);
        SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
        while (iterator.next()) {
          float rate = iterator.getInt(2) / 96.0f;
          columnBuilders.get(0).writeBinary(new Binary(method.getBytes()));
          columnBuilders.get(1).writeFloat(rate);
          columnBuilders.get(2).writeBinary(new Binary(iterator.getString(1).getBytes()));
        }
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        System.out.println(e.getMessage());
      }
    }
  }
}
