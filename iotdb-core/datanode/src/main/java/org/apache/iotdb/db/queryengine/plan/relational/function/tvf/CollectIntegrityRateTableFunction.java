package org.apache.iotdb.db.queryengine.plan.relational.function.tvf;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
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
import org.apache.tsfile.utils.Binary;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** collect_integrity_rate('electric','2025-08-01') 数据采集成功率分析 */
public class CollectIntegrityRateTableFunction implements TableFunction {

  private final String TABLE = "table";
  private final String DATE = "date";

  @Override
  public List<ParameterSpecification> getArgumentsSpecifications() {
    return Arrays.asList(
        ScalarParameterSpecification.builder().name(TABLE).type(Type.STRING).build(),
        ScalarParameterSpecification.builder().name(DATE).type(Type.STRING).build());
  }

  @Override
  public TableFunctionAnalysis analyze(Map<String, Argument> arguments) throws UDFException {
    DescribedSchema schema =
        DescribedSchema.builder()
            .addField("date", Type.STRING)
            .addField("frozen_success_rate", Type.FLOAT)
            .addField("load_success_rate", Type.FLOAT)
            .build();
    MapTableFunctionHandle handle =
        new MapTableFunctionHandle.Builder()
            .addProperty(TABLE, ((ScalarArgument) arguments.get(TABLE)).getValue())
            .addProperty(DATE, ((ScalarArgument) arguments.get(DATE)).getValue())
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
        return new CollectIntegrityRateProcessor(
            (String) handle.getProperty(TABLE), (String) handle.getProperty(DATE));
      }
    };
  }

  private class CollectIntegrityRateProcessor implements TableFunctionLeafProcessor {
    private String tableName;
    private String date;
    private ITableSession sessionFrozen;
    private ITableSession sessionLoaded;
    private SessionDataSet sessionDataSetFrozen;
    private SessionDataSet sessionDataSetLoaded;
    private boolean isFinish = false;

    CollectIntegrityRateProcessor(String tableName, String date) {
      this.tableName = tableName;
      this.date = date;
    }

    @Override
    public void beforeStart() {
      TEndPoint endPoint = IoTDBDescriptor.getInstance().getConfig().getAddressAndPort();
      String address = endPoint.ip + ":" + endPoint.port;
      try {
        sessionFrozen =
            new TableSessionBuilder()
                .nodeUrls(Collections.singletonList(address))
                .username("root")
                .password("root")
                .database("nxgw")
                .build();
        sessionLoaded =
            new TableSessionBuilder()
                .nodeUrls(Collections.singletonList(address))
                .username("root")
                .password("root")
                .database("nxgw")
                .build();
      } catch (IoTDBConnectionException e) {
        System.out.println(e.getMessage());
      }
    }

    @Override
    public void process(List<ColumnBuilder> columnBuilders) {
      columnBuilders.get(0).writeBinary(new Binary(date.getBytes()));
      processFrozen(columnBuilders);
      processLoad(columnBuilders);
      isFinish = true;
    }

    @Override
    public boolean isFinish() {
      return isFinish;
    }

    @Override
    public void beforeDestroy() {
      try {
        if (sessionDataSetLoaded != null) {
          sessionDataSetLoaded.close();
        }
        if (sessionDataSetFrozen != null) {
          sessionDataSetFrozen.close();
        }
        if (sessionLoaded != null) {
          sessionLoaded.close();
        }
        if (sessionFrozen != null) {
          sessionFrozen.close();
        }
      } catch (Exception e) {
        System.out.println(e.getMessage());
      }
    }

    private void processFrozen(List<ColumnBuilder> columnBuilders) {
      try {
        String frozenSQL =
            "select pn_id, f1 from " + tableName + " where time = " + date + "T00:00:00";
        sessionDataSetFrozen = sessionFrozen.executeQueryStatement(frozenSQL);
        SessionDataSet.DataIterator iterator = sessionDataSetFrozen.iterator();
        long countAll = 0;
        long countTarget = 0;
        while (iterator.next()) {
          countAll++;
          if (!iterator.isNull("f1")) {
            countTarget++;
          } else {
            System.out.println(iterator.getFloat("f1"));
          }
        }
        float rate = countTarget / (countAll == 0 ? 1.0f : countAll);
        columnBuilders.get(1).writeFloat(rate);
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        System.out.println(e.getMessage());
      }
    }

    private void processLoad(List<ColumnBuilder> columnBuilders) {
      try {
        String loadedSQL =
            "select count(f1) from "
                + tableName
                + " where time >= "
                + date
                + "T00:00:00 and time <"
                + date
                + "T23:59:59 group by pn_id";
        sessionDataSetLoaded = sessionLoaded.executeQueryStatement(loadedSQL);
        SessionDataSet.DataIterator iterator = sessionDataSetLoaded.iterator();
        long countAll = 0;
        long countTarget = 0;
        while (iterator.next()) {
          countAll++;
          if (iterator.getInt(1) > 90) {
            countTarget++;
          }
        }
        float rate = countTarget / (countAll == 0 ? 1.0f : countAll);
        columnBuilders.get(2).writeFloat(rate);
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        System.out.println(e.getMessage());
      }
    }
  }
}
