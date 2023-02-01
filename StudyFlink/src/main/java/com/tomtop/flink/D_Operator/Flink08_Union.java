package com.tomtop.flink.D_Operator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author:txf
 * @Date:2022/12/11 10:43
 * union
 * 1. union可以一次连接多个流
 * 2. 多个流的数据类型必须一致
 */
public class Flink08_Union {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        DataStreamSource<Integer> integerDataStreamSource1 = env.fromElements(1, 2, 3, 4, 5, 6, 7);
        DataStreamSource<Integer> integerDataStreamSource2 = env.fromElements(8,9,10);

        integerDataStreamSource1
                .union(integerDataStreamSource2)
                .map(new MapFunction<Integer, String>() {
                    @Override
                    public String map(Integer value) throws Exception {
                        return value + "<>";
                    }
                })
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
