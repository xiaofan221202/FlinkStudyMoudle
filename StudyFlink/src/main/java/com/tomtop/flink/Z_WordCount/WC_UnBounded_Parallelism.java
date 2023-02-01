package com.tomtop.flink.Z_WordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author:txf
 * @Date:2022/12/9 15:17
 * flink中操作链的优化是自动实现:
 *     算子与算子之间: one-to-one
 *     算子的并行度一样
 *
 * .startNewChain()
 *     开启一个新链: 当前算子不会和前面的优化在一起，如果后面的算子满足优化条件, 也会与当前算子优化到一起
 *
 * .disableChaining()
 *     当前算子不参与任何链的优化
 *
 *  env.disableOperatorChaining()
 *     当前应用所有算子都不进行优化
 *
 *  实际生产环境下, 尽量不要禁止优化: 优化只有好处没有坏处
 * -----------------

 * 在flink中, 有4种办法设置并行度
 * 1. 在flink-conf.yaml 配置文件中设置算子的默认并行度
 *         parallelism.default: 1
 *
 * 2. 提交job的是时候通过参数设置
 *         bin/flink run -d -p 2 ...
 *
 * 3. 在代码中通过环境设置
 *        env.setParallelism(1);
 *        注意: 在flink中socket这个source他的并行度只能是1.
 *
 * 4. 单独给每个算子设置并行度
 *        算子.setParallelism(3)
 */
public class WC_UnBounded_Parallelism {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",2000); //设置job查看的端口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);//全局设置并行度
        env.disableOperatorChaining(); //不开启算子链优化

        DataStreamSource<String> socketStream = env.socketTextStream("localhost", 9000)
                .setParallelism(1);//单独设置并行度，socket这个source算子并行度只能是1

        SingleOutputStreamOperator<Tuple2<String, Long>> resultStream = socketStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                for (String word : line.split(" ")) {
                    collector.collect(word);
                }
            }
        })
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String word) throws Exception {
                        return Tuple2.of(word, 1L);
                    }
                })
                //.startNewChain()
                //.disableChaining()
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> stringLongTuple2) throws Exception {
                        return stringLongTuple2.f0;
                    }
                })
                .sum(1);

        resultStream.print();

        try {
            env.execute("test socketStream wordcount and parallelism");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
