package com.tomtop.flink.Z_WordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author:txf
 * @Date:2022/12/9 14:22
 */
public class WC_UnBounded {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        DataStreamSource<String> socketStream = env.socketTextStream("localhost", 9000);

        SingleOutputStreamOperator<String> wordStream = socketStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                String[] splitWord = line.split(" ");
                for (String word : splitWord) {
                    collector.collect(word);
                }
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Long>> tuple2Stream = wordStream.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String word) throws Exception {
                return Tuple2.of(word, 1L);
            }
        });
        KeyedStream<Tuple2<String, Long>, String> keyedStream = tuple2Stream.keyBy(new KeySelector<Tuple2<String, Long>, String>() {
            @Override
            public String getKey(Tuple2<String, Long> stringLongTuple2) throws Exception {
                return stringLongTuple2.f0;
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Long>> resultStream = keyedStream.sum(1);
        resultStream.print();

        try {
            env.execute("test from socketStream wordcount");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
