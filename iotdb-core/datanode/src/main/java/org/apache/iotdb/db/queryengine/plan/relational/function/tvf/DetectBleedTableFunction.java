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
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.TableFunction;
import org.apache.iotdb.udf.api.relational.table.MapTableFunctionHandle;
import org.apache.iotdb.udf.api.relational.table.TableFunctionAnalysis;
import org.apache.iotdb.udf.api.relational.table.TableFunctionHandle;
import org.apache.iotdb.udf.api.relational.table.TableFunctionProcessorProvider;
import org.apache.iotdb.udf.api.relational.table.argument.Argument;
import org.apache.iotdb.udf.api.relational.table.argument.DescribedSchema;
import org.apache.iotdb.udf.api.relational.table.processor.TableFunctionLeafProcessor;
import org.apache.iotdb.udf.api.relational.table.specification.ParameterSpecification;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.Binary;

import java.time.ZoneId;
import java.util.*;

/** select * form detect_bleed(); */
public class DetectBleedTableFunction implements TableFunction {

  @Override
  public List<ParameterSpecification> getArgumentsSpecifications() {
    return new ArrayList<ParameterSpecification>() {};
  }

  @Override
  public TableFunctionAnalysis analyze(Map<String, Argument> arguments) throws UDFException {
    DescribedSchema schema =
        DescribedSchema.builder()
            .addField("is_health", Type.BOOLEAN)
            .addField("time", Type.TIMESTAMP)
            .addField("rank", Type.STRING)
            .addField("part", Type.STRING)
            .addField("value", Type.FLOAT)
            .build();
    MapTableFunctionHandle handle = new MapTableFunctionHandle.Builder().build();
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
        return new BleedProcessor();
      }
    };
  }

  private class BleedProcessor implements TableFunctionLeafProcessor {
    List<String> sqlList = null;

    IClientSession internalSession = null;
    Coordinator coordinator = Coordinator.getInstance();
    SessionManager sessionManager = SessionManager.getInstance();
    Metadata metadata = LocalExecutionPlanner.getInstance().metadata;
    SqlParser sqlParser = new SqlParser();

    List<Float> precool_1 = new ArrayList<>(17200);
    List<Float> precool_2 = new ArrayList<>(17200);
    List<Long> time_data = new ArrayList<>(17200);
    List<Float> phase_data = new ArrayList<>(4300);
    List<Float> pack1_status = new ArrayList<>(17200);
    List<Float> pack2_status = new ArrayList<>(17200);

    List<Float> diffCache1 = null;
    List<Float> diffCache2 = null;

    boolean finish = false;

    BleedProcessor() {
      sqlList = new ArrayList<>();
      sqlList.add("select time, value from my_precool_press1");
      sqlList.add("select time, value from precool_press2");
      sqlList.add("select time, value from phase1");
      sqlList.add("select time, value from pack1_pb");
      sqlList.add("select time, value from pack2_pb");
      diffCache1 = new ArrayList<>();
      diffCache2 = new ArrayList<>();
    }

    public void executeQueryAndGetTsBlockAndFillLists(int sqlListIndex) throws IoTDBException {
      String sql = sqlList.get(sqlListIndex);
      long queryId = sessionManager.requestQueryId(internalSession, sessionManager.requestStatementId(internalSession));
      Throwable throwable = null;

      try {
        Statement statement = sqlParser.createStatement(sql, internalSession.getZoneId(), internalSession);
        ExecutionResult result = coordinator.executeForTableModel(
                statement,
                sqlParser,
                internalSession,
                queryId,
                sessionManager.getSessionInfoOfTableModel(internalSession),
                sql,
                metadata,
                Long.MAX_VALUE,
                false
        );
        if (result.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
        && result.status.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
          throw new IoTDBException(
                  result.status.getMessage(),
                  result.status.getCode()
          );
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
          Column timeColumn = tsBlock.getColumn(0);
          Column valueColumn = tsBlock.getColumn(1);
          int positionCount = tsBlock.getPositionCount();

          if (sqlListIndex == 0) {
            for (int i = 0; i < positionCount; i++) {
              if (valueColumn.isNull(i)) {
                continue;
              }
              float value = valueColumn.getFloat(i);
              precool_1.add(value);
            }
          } else if (sqlListIndex == 1) {
            for (int i = 0; i < positionCount; i++) {
              if (valueColumn.isNull(i)) {
                continue;
              }
              long time = timeColumn.getLong(i);
              float value = valueColumn.getFloat(i);
              precool_2.add(value);
              time_data.add(time);
            }
          } else if (sqlListIndex == 2) {
            for (int i = 0; i < positionCount; i++) {
              if (valueColumn.isNull(i)) {
                continue;
              }
              float value = valueColumn.getFloat(i);
              phase_data.add(value);
            }
          } else if (sqlListIndex == 3) {
            for (int i = 0; i < positionCount; i++) {
              if (valueColumn.isNull(i)) {
                continue;
              }
              float value = valueColumn.getFloat(i);
              pack1_status.add(value);
            }

          } else if (sqlListIndex == 4) {
            for (int i = 0; i < positionCount; i++) {
              if (valueColumn.isNull(i)) {
                continue;
              }
              float value = valueColumn.getFloat(i);
              pack2_status.add(value);
            }
          }

/*          for (int i = 0; i < positionCount; i++) {
            if (valueColumn.isNull(i)) {
              continue;
            }
            long time = timeColumn.getLong(i);
            float value = valueColumn.getFloat(i);
            if (sqlListIndex == 0) {
              precool_1.add(value);
            } else if (sqlListIndex == 1) {
              precool_2.add(value);
              time_data.add(time);
            } else if (sqlListIndex == 2) {
              phase_data.add(value);
            } else if (sqlListIndex == 3) {
              pack1_status.add(value);
            } else if (sqlListIndex == 4) {
              pack2_status.add(value);
            }
          }*/
        }
      } catch (Throwable t) {
        throwable = t;
        throw new RuntimeException("Failed to execute query: " + sql, t);
      } finally {
        coordinator.cleanupQueryExecution(queryId, null, throwable);
      }
    }

    @Override
    public void beforeStart() {
      try {
        long b1 = System.currentTimeMillis();
        internalSession = new InternalClientSession("query");
        internalSession.setDatabaseName("a320");
        sessionManager.registerSession(internalSession);
        sessionManager.supplySession(
                internalSession,
                -1,
                "root",
                ZoneId.systemDefault(),
                IoTDBConstant.ClientVersion.V_1_0
        );
        for (int i = 0; i < 5; i++) {
          executeQueryAndGetTsBlockAndFillLists(i);
        }
        sessionManager.closeSession(internalSession, coordinator::cleanupQueryExecution);
        sessionManager.removeCurrSession();
        long b2 = System.currentTimeMillis();
        System.out.println("init list:" + (b2 - b1) + "ms");
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void process(List<ColumnBuilder> columnBuilders) {
      // 第一阶段：数据预处理
      long t3s = System.currentTimeMillis();
      for (int idx = 4; idx < precool_1.size(); idx++) {
        int phaseIdx = idx / 4;

        // 边界检查
        if (phaseIdx >= phase_data.size()) {
          continue;
        }
        if (idx >= pack1_status.size() || idx >= pack2_status.size()) {
          continue;
        }

        // 检测条件判断
        if (phase_data.get(phaseIdx) != 10
            && pack1_status.get(idx) == 1
            && pack2_status.get(idx) == 1) { // 非特定飞行阶段 and 组件1工作 and 组件2工作
          List<Float> window1 = new ArrayList<>();
          List<Float> window2 = new ArrayList<>();
          // 计算 5s 窗口
          for (int innerIdx = idx - 4; innerIdx < idx + 1; innerIdx++) {
            window1.add(precool_1.get(innerIdx));
            window2.add(precool_2.get(innerIdx));
          }
          // 记录波动插值
          diffCache1.add(findMaxValue(window1) - findMinValue(window1));
          diffCache2.add(findMaxValue(window2) - findMinValue(window2));
        }
      }
      long t3e = System.currentTimeMillis();
      System.out.println("get diffCache costs:" + (t3e - t3s) + "ms");

      // 第二阶段：波动分析
      if (diffCache1.size() > 60) {
        float minPressure = 3.0f; // 压力波动阈值
        float total1 = 0.0f;
        float total2 = 0.0f;

        // 滑动窗口分析(60s窗口)
        long t4s = System.currentTimeMillis();
        for (int windowEnd = 59; windowEnd < diffCache1.size(); windowEnd++) {
          List<Float> window1 = new ArrayList<>();
          List<Float> window2 = new ArrayList<>();
          for (int innerIdx = windowEnd - 59; innerIdx < windowEnd + 1; innerIdx++) {
            window1.add(diffCache1.get(innerIdx));
            window2.add(diffCache2.get(innerIdx));
          }
          // 左翼分析
          float minValue1 = findMinValue(window1);
          if (minValue1 >= minPressure) {
            total1 += minValue1;
          }
          // 右翼分析
          float minValue2 = findMinValue(window2);
          if (minValue2 >= minPressure) {
            total2 += minValue2;
          }
        }
        long t4e = System.currentTimeMillis();
        System.out.println("window analyze costs:" + (t4e - t4s) + "ms");

        // 计算平均波动值
        float avg1 = total1 / diffCache1.size();
        float avg2 = total2 / diffCache1.size();

        // 事件生成逻辑
        long t5s = System.currentTimeMillis();
        if (avg1 <= 1.0 && avg2 <= 1.0) {
          columnBuilders.get(0).writeBoolean(true);
          columnBuilders.get(1).writeLong(1);
          columnBuilders.get(2).writeBinary(new Binary("".getBytes()));
          columnBuilders.get(3).writeBinary(new Binary("".getBytes()));
          columnBuilders.get(4).writeFloat(0.0f);
        } else {
          columnBuilders.get(0).writeBoolean(false);
          columnBuilders.get(1).writeLong(time_data.get(time_data.size() - 1));
          columnBuilders.get(2).writeBinary(new Binary("CL1".getBytes()));
          if (avg1 > 1.0) {
            columnBuilders.get(3).writeBinary(new Binary("左侧".getBytes()));
            columnBuilders.get(4).writeFloat(Math.round(avg1 * 100.0) / 100.0f);
          } else if (avg2 > 1.0) {
            columnBuilders.get(3).writeBinary(new Binary("右侧".getBytes()));
            columnBuilders.get(4).writeFloat(Math.round(avg2 * 100.0) / 100.0f);
          }
        }
        long t5e = System.currentTimeMillis();
        System.out.println("get result costs:" + (t5e - t5s) + "ms");

        finish = true;
      }
    }

    @Override
    public boolean isFinish() {
      return finish;
    }

    @Override
    public void beforeDestroy() {}
  }

  public static float findMinValue(List<Float> values) {
    if (values == null || values.size() == 0) {
      return 0.0f;
    }
    float minValue = values.get(0);
    for (int i = 1; i < values.size(); i++) {
      if (values.get(i) < minValue) {
        minValue = values.get(i);
      }
    }
    return minValue;
  }

  public static float findMaxValue(List<Float> values) {
    if (values == null || values.size() == 0) {
      return 0.0f;
    }
    float maxValue = values.get(0);
    for (int i = 1; i < values.size(); i++) {
      if (values.get(i) > maxValue) {
        maxValue = values.get(i);
      }
    }
    return maxValue;
  }
}
