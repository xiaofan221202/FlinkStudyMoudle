package com.tomtop.flink.E_Sink;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @Author:txf
 * @Date:2022/12/11 20:22
 */
public class SInk_Doris_String {
    public static void main(String[] args) throws Exception {

        //create and set environment
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        env.enableCheckpointing(10000);
/*      //way of setting up one
        env.enableCheckpointing(1000,CheckpointingMode.AT_LEAST_ONCE);
        //way of setting up two
        env.getCheckpointConfig().setCheckpointInterval(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointConfig.DEFAULT_MODE);*/



        //DorisSink Connect Build
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder
                .setFenodes("172.18.1.18:8030")  //FE节点IP和端口
                .setTableIdentifier("qulv2.erp_order_id")
                .setUsername("tomtop")
                .setPassword("qulv123");

        DorisExecutionOptions.Builder executionBuilder = DorisExecutionOptions.builder();
        executionBuilder
                .setLabelPrefix("label-doris");//Stream load导入使用的label前缀。2pc场景下要求全局唯一 ，用来保证Flink的EOS(Exactly-Once Semantics)语义。
        DorisReadOptions.Builder readOptionsBuilder = DorisReadOptions.builder();
        DorisSink.Builder<String> builder = DorisSink.builder();
        builder
                .setDorisReadOptions(readOptionsBuilder.build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setSerializer(new SimpleStringSerializer()) //序列化是String，如果是其他得自定义序列化
                .setDorisOptions(dorisBuilder.build());


        //Mock String Source And Writer Sink
        ArrayList<Tuple2<String,Integer>> data = new ArrayList<>();
        data.add(new Tuple2<>("doris",1));
        DataStreamSource<Tuple2<String, Integer>> dataSource = env.fromCollection(data);
        SingleOutputStreamOperator<String> mapSource = dataSource.map(new MapFunction<Tuple2<String, Integer>, String>() {
            @Override
            public String map(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0 + "/t" + stringIntegerTuple2.f1;
            }
        });

        mapSource.print();

//        mapSource.sinkTo(builder.build());

        env.execute();

    }
}
