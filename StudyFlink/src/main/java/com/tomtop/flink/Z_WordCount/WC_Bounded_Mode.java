package com.tomtop.flink.Z_WordCount;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author:txf
 * @Date:2022/12/9 15:18
 */
public class WC_Bounded_Mode {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
//        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
//        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);//流批一体

        DataStreamSource<String> sourceStream = env.readTextFile("src/input/word.txt");

        sourceStream
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String line, Collector<String> collector) throws Exception {
                        for (String word : line.split(" ")) {
                            collector.collect(word);
                        }
                    }
                })
                .map(new MapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String word) throws Exception {
                        return Tuple2.of(word,1L);
                    }
                })
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> stringLongTuple2) throws Exception {
                        return stringLongTuple2.f0;
                    }
                })
                .sum(1)
                .print();

        try {
            env.execute("test readstream wordcount mode");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
