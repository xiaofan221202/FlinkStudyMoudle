package com.tomtop.flink.Z_WordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author:txf
 * @Date:2022/12/9 14:51
 */
public class WC_UnBounded_Lambda {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> socketStream = env.socketTextStream("localhost", 9000);

        SingleOutputStreamOperator<String> lineStream = socketStream.flatMap((FlatMapFunction<String, String>) (line, collector) -> {
            for (String word : line.split(" ")) {
                collector.collect(word);
            }
        }).returns(Types.STRING);  //flatMap使用lambda写法后会报错，原因是泛型擦除。因此不知道flink中的类型。这里手动指定类型，注意类型Types导包导flink的。
        SingleOutputStreamOperator<Tuple2<String, Long>> tupleStream = lineStream.map((MapFunction<String, Tuple2<String, Long>>)
                word -> Tuple2.of(word, 1L))
                .returns(Types.TUPLE(Types.STRING,Types.LONG));//map算子使用lambda写法会报错，原因泛型擦除，这里手动指定类型。
        KeyedStream<Tuple2<String, Long>, String> keyedStream = tupleStream.keyBy((KeySelector<Tuple2<String, Long>, String>)
                stringLongTuple2 -> stringLongTuple2.f0);//方法引用写法，只能在keyby这个算子中使用
        SingleOutputStreamOperator<Tuple2<String, Long>> resultStream = keyedStream.sum(1);
        resultStream.print();

        try {
            env.execute("test socketStream wordcount lambda");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
