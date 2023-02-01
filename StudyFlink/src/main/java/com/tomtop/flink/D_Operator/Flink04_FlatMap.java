package com.tomtop.flink.D_Operator;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author:txf
 * @Date:2022/12/11 10:05
 */
public class Flink04_FlatMap {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env.fromElements(1,2,3,4,5,6,7)
                .flatMap(new FlatMapFunction<Integer, Integer>() {
                    @Override
                    public void flatMap(Integer value, Collector<Integer> out) throws Exception {
                        //每调用一次collect方法，后面流就会添加一个元素(重点理解)
                        if (value % 2 ==0){
                            out.collect(value* value);
                            out.collect(value* value* value);
                        }
                    }
                }).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
