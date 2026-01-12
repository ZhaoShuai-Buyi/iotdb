package org.apache.iotdb.db.queryengine.plan.relational.function.tvf;

import org.apache.iotdb.udf.api.relational.TableFunction;
import org.apache.iotdb.udf.api.relational.table.processor.TableFunctionLeafProcessor;

public class DetectBleedTableFunction implements TableFunction {
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

        ParamAlignProcessor() {

        }

        @Override
        public void beforeStart() {
            try {
                long t1 = System.currentTimeMillis();
                // 获取 metadata 迭代器
                flightInfoSession =
                        new TableSessionBuilder()
                                .nodeUrls(Collections.singletonList(address))
                                .username("root")
                                .password("root")
                                .database("b777")
                                .build();
                flightInfoDataSet =
                        flightInfoSession.executeQueryStatement(
                                "select \"flight/datetime/startrecordingdatetime\", \"flight/datetime/endrecordingdatetime\" from flight_metadata where \"aircraft/tail\" = '"
                                        + aircraft
                                        + "' and \"flight/datetime/startrecordingdatetime\" >= '"
                                        + startTime
                                        + "' and \"flight/datetime/endrecordingdatetime\" <= '"
                                        + endTime
                                        + "'"); // 后续可选加入地点过滤条件
                flightInfoDataIterator = flightInfoDataSet.iterator();

                // 用同一批 session
                for (int i = 0; i < size; i++) {
                    flightSession.add(
                            new TableSessionBuilder()
                                    .nodeUrls(Collections.singletonList(address))
                                    .username("root")
                                    .password("root")
                                    .database("b777")
                                    .build());
                }
                long t2 = System.currentTimeMillis();
                System.out.println("before start cost: " + (t2 - t1) + "ms");
            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void process(List<ColumnBuilder> columnBuilders) {

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
                if (flightSession != null) {
                    for (int i = 0; i < flightSession.size(); i++) {
                        if (flightSession.get(i) != null) {
                            flightSession.get(i).close();
                        }
                    }
                }
                flightInfoDataSet.close();
                flightInfoSession.close();
            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean isFinish() {
            return finish;
        }
    }
}
