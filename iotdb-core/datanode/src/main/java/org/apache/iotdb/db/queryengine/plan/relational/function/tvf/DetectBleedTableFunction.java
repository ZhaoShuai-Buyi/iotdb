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
import org.apache.iotdb.udf.api.relational.table.processor.TableFunctionLeafProcessor;
import org.apache.iotdb.udf.api.relational.table.specification.ParameterSpecification;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.utils.Binary;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * select * form detect_bleed();
 */
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

    List<ITableSession> sourceSession = null;
    List<SessionDataSet> sourceDataSet = null;
    List<SessionDataSet.DataIterator> sourceDataIterator = null;

    List<Float> diffCache1 = null;
    List<Float> diffCache2 = null;

    boolean finish = false;

    BleedProcessor() {
      sqlList = new ArrayList<>();
      sqlList.add("select time, value from precool_press1");
      sqlList.add("select time, value from precool_press2");
      sqlList.add("select time, value from phase1");
      sqlList.add("select time, value from pack1_pb");
      sqlList.add("select time, value from pack2_pb");
      sourceSession = new ArrayList<>();
      sourceDataSet = new ArrayList<>();
      sourceDataIterator = new ArrayList<>();
      diffCache1 = new ArrayList<>();
      diffCache2 = new ArrayList<>();
    }

    @Override
    public void beforeStart() {
      try {
        TEndPoint endPoint = IoTDBDescriptor.getInstance().getConfig().getAddressAndPort();
        String address = endPoint.getIp() + ":" + endPoint.getPort();
        for (int i = 0; i < 5; i++) {
          sourceSession.add(
              new TableSessionBuilder()
                  .nodeUrls(Collections.singletonList(address))
                  .username("root")
                  .password("root")
                  .database("a320")
                  .build());
          sourceDataSet.add(sourceSession.get(i).executeQueryStatement(sqlList.get(i)));
          sourceDataIterator.add(sourceDataSet.get(i).iterator());
        }
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void process(List<ColumnBuilder> columnBuilders) {
      List<Float> precool_1 = new ArrayList<>();
      List<Float> precool_2 = new ArrayList<>();
      List<Long> time_data = new ArrayList<>();
      List<Float> phase_data = new ArrayList<>();
      List<Float> pack1_status = new ArrayList<>();
      List<Float> pack2_status = new ArrayList<>();
      try {
        while (sourceDataIterator.get(2).next()) {
          phase_data.add(sourceDataIterator.get(2).getFloat(2));
          for (int i = 0; i < 4; i++) {
            sourceDataIterator.get(0).next();
            sourceDataIterator.get(1).next();
            sourceDataIterator.get(3).next();
            sourceDataIterator.get(4).next();
            precool_1.add(sourceDataIterator.get(0).getFloat(2));
            precool_2.add(sourceDataIterator.get(1).getFloat(2));
            time_data.add(sourceDataIterator.get(1).getLong(1));
            pack1_status.add(sourceDataIterator.get(3).getFloat(2));
            pack2_status.add(sourceDataIterator.get(4).getFloat(2));
          }
        }

        // 第一阶段：数据预处理
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

        System.out.println("diff_cache length: " + diffCache1.size());

        // 第二阶段：波动分析
        if (diffCache1.size() > 60) {
          float minPressure = 3.0f; // 压力波动阈值
          float total1 = 0.0f;
          float total2 = 0.0f;

          // 滑动窗口分析(60s窗口)
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

          // 计算平均波动值
          float avg1 = total1 / diffCache1.size();
          float avg2 = total2 / diffCache1.size();

          System.out.println("avg1: " + avg1);
          System.out.println("avg2: " + avg2);

          // 事件生成逻辑
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

          finish = true;
        }

      } catch (IoTDBConnectionException | StatementExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public boolean isFinish() {
      return finish;
    }

    @Override
    public void beforeDestroy() {
      try {
        if (sourceDataSet != null) {
          for (int i = 0; i < sourceDataSet.size(); i++) {
            sourceDataSet.get(i).close();
          }
        }
        if (sourceSession != null) {
          for (int i = 0; i < sourceSession.size(); i++) {
            sourceSession.get(i).close();
          }
        }
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        throw new RuntimeException(e);
      }
    }
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
