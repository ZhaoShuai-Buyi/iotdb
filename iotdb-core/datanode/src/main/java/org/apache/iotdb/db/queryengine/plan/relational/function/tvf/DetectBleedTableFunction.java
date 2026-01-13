package org.apache.iotdb.db.queryengine.plan.relational.function.tvf;

import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.TableFunction;
import org.apache.iotdb.udf.api.relational.table.TableFunctionAnalysis;
import org.apache.iotdb.udf.api.relational.table.TableFunctionHandle;
import org.apache.iotdb.udf.api.relational.table.TableFunctionProcessorProvider;
import org.apache.iotdb.udf.api.relational.table.argument.Argument;
import org.apache.iotdb.udf.api.relational.table.processor.TableFunctionLeafProcessor;
import org.apache.iotdb.udf.api.relational.table.specification.ParameterSpecification;
import org.apache.tsfile.block.column.ColumnBuilder;

import java.util.List;
import java.util.Map;

public class DetectBleedTableFunction implements TableFunction {

    @Override
    public List<ParameterSpecification> getArgumentsSpecifications() {
        return null;
    }

    @Override
    public TableFunctionAnalysis analyze(Map<String, Argument> arguments) throws UDFException {
        return null;
    }

    @Override
    public TableFunctionHandle createTableFunctionHandle() {
        return null;
    }

    @Override
    public TableFunctionProcessorProvider getProcessorProvider(TableFunctionHandle tableFunctionHandle) {
        return new TableFunctionProcessorProvider() {
            @Override
            public TableFunctionLeafProcessor getSplitProcessor() {
                return new BleedProcessor();
            }
        };
    }

    private class BleedProcessor implements TableFunctionLeafProcessor {
        String sql1 = "select time, value from precool_press1";
        String sql2 = "select time, value from precool_press2";
        String sql3 = "select time, value from phase1";
        String sql4 = "select time, value from pack1_pb";
        String sql5 = "select time, value from pack2_pb";

        boolean finish = false;


        @Override
        public void beforeStart() {
            TableFunctionLeafProcessor.super.beforeStart();
        }

        @Override
        public void process(List<ColumnBuilder> columnBuilders) {

        }

        @Override
        public boolean isFinish() {
            return finish;
        }

        @Override
        public void beforeDestroy() {
            TableFunctionLeafProcessor.super.beforeDestroy();
        }
    }

}