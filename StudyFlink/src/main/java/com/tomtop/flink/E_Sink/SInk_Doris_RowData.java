package com.tomtop.flink.E_Sink;


import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.RowDataSerializer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;


import java.util.Properties;

/**
 * @Author:txf
 * @Date:2022/12/11 20:22
 */
public class SInk_Doris_RowData {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        env.enableCheckpointing(10000);

        //Doris Sink Option Set
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder
                .setFenodes("172.18.1.18:8030")
                .setTableIdentifier("qulv2.erp_order_id")
                .setUsername("tomtop")
                .setPassword("qulv123");

        //Json Format To StreamLoad
        Properties prop = new Properties();
        prop.setProperty("format","json");
        prop.setProperty("read_json_by_line","true");

        DorisExecutionOptions.Builder executionBuilder = DorisExecutionOptions.builder();
        executionBuilder
                .setLabelPrefix("label-doris")//Stream load导入使用的label前缀。2pc场景下要求全局唯一 ，用来保证Flink的EOS(Exactly-Once Semantics)语义。
                .setStreamLoadProp(prop); //StreamLoad导入的参数格式属性

        //Flink RowData's Schema
        String[] fields = {"city","longitude","latitude"};
        DataType[] types = {DataTypes.VARCHAR(256), DataTypes.DOUBLE(), DataTypes.DOUBLE()};

        DorisSink.Builder<RowData> builder = DorisSink.builder();
        builder
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setDorisOptions(dorisBuilder.build())
                .setSerializer(RowDataSerializer.builder()    //serialize according to rowdata
                        .setFieldNames(fields)
                        .setType("json")           //json format
                        .setFieldType(types)
                        .build());

        //Mock RowData Source And Write Sink
        DataStream<RowData> source = env.fromElements("")
                .map(new MapFunction<String, RowData>() {
                    @Override
                    public RowData map(String value) throws Exception {
                        GenericRowData genericRowData = new GenericRowData(3);
                        genericRowData.setField(0, StringData.fromString("beijing"));
                        genericRowData.setField(1, 116.405419);
                        genericRowData.setField(2, 39.916927);
                        return genericRowData;
                    }
                });

        source.print();

//        source.sinkTo(builder.build());

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
