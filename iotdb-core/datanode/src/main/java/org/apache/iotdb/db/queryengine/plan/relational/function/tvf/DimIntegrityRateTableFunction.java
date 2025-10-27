package org.apache.iotdb.db.queryengine.plan.relational.function.tvf;

import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.TableFunction;
import org.apache.iotdb.udf.api.relational.table.TableFunctionAnalysis;
import org.apache.iotdb.udf.api.relational.table.TableFunctionHandle;
import org.apache.iotdb.udf.api.relational.table.TableFunctionProcessorProvider;
import org.apache.iotdb.udf.api.relational.table.argument.Argument;
import org.apache.iotdb.udf.api.relational.table.specification.ParameterSpecification;

import java.util.List;
import java.util.Map;

/**
 * dim_integrity_rate('electric','2025-08-01','all' | 'single')
 * 数据采集完整性分析
 */
public class DimIntegrityRateTableFunction implements TableFunction {
  @Override
  public List<ParameterSpecification> getArgumentsSpecifications() {
    return List.of();
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
  public TableFunctionProcessorProvider getProcessorProvider(
      TableFunctionHandle tableFunctionHandle) {
    return null;
  }
}
