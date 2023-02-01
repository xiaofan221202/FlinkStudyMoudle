package com.tomtop.doris;


import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.cfg.DorisSink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.*;


/**
 * @Author:txf
 * @Date:2022/12/12 17:34
 */
public class DataStreamRowDataDorisSink {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        SingleOutputStreamOperator<RowData> mapSource = env.socketTextStream("localhost", 9000)
                .map(new MapFunction<String, RowData>() {
                    @Override
                    public RowData map(String s) throws Exception {
                        GenericRowData genericRowData = new GenericRowData(4);
                        genericRowData.setField(0, 88);
                        genericRowData.setField(1, new Short("8"));
                        genericRowData.setField(2, StringData.fromString("flink-stream"));
                        genericRowData.setField(3, 8L);
                        return genericRowData;
                    }
                });

        LogicalType[] types = {new IntType(), new SmallIntType(), new VarCharType(32), new BigIntType()};
        String[] fields = {"siteid", "citycode", "username", "pv"};

        mapSource.addSink(DorisSink.sink(
                fields,
                types,
                DorisReadOptions.builder().build(),
                DorisExecutionOptions.builder().setBatchSize(3).setBatchIntervalMs(0l).setMaxRetries(3).build(),
                DorisOptions.builder().setFenodes("172.18.1.18:8030").setTableIdentifier("qulv2.order_info_sink").setUsername("tomtop").setPassword("qulv123").build()
        ));

        env.execute();


    }
}
