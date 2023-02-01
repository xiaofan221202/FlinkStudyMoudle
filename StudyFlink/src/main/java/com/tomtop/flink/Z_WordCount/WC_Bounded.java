package com.tomtop.flink.Z_WordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 *
 * @Author:txf
 * @Date:2022/12/9 14:02
 */
public class WC_Bounded {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        DataStreamSource<String> readStream = env.readTextFile("src/input/word.txt");

        SingleOutputStreamOperator<String> wordStream = readStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                for (String word : line.split(" ")) {
                    collector.collect(word);
                }
            }
        });
//        readStream.print();
        SingleOutputStreamOperator<Tuple2<String, Long>> Tuple2Stream = wordStream.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String word) throws Exception {
                return Tuple2.of(word, 1L);
            }
        });
//        Tuple2Stream.print();
        KeyedStream<Tuple2<String, Long>, String> keyedStream = Tuple2Stream.keyBy(new KeySelector<Tuple2<String, Long>, String>() {
            @Override
            public String getKey(Tuple2<String, Long> stringLongTuple2) throws Exception {
                return stringLongTuple2.f0;
            }
        });
//        keyedStream.print();
        SingleOutputStreamOperator<Tuple2<String, Long>> resultStream = keyedStream.sum(1);
        resultStream.print();
        try {
            env.execute("test readStream wordcount");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

