package com.tomtop.flink.D_Operator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author:txf
 * @Date:2022/12/11 9:35
 */
public class Flink01_Map {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        //java中的写法：
        env.fromElements(1,2,3,4,5,6).map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value * value;
            }
        }).print();

        //lambda表达式写法：
        //env.fromElements(1,2,3,4,5,6).map( value -> value * value).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
