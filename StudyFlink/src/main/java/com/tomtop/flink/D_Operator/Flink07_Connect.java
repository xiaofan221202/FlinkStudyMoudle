package com.tomtop.flink.D_Operator;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @Author:txf
 * @Date:2022/12/11 10:30
 * connect:
 * 1. connect一次只能连接两个流
 * 2. 两个流的类型可以不一样
 */
public class Flink07_Connect {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        DataStreamSource<Integer> integerDataStreamSource = env.fromElements(1, 2, 3, 4, 5, 6, 7);
        DataStreamSource<String> stringDataStreamSource = env.fromElements("a", "b", "c");

        integerDataStreamSource.connect(stringDataStreamSource)
                .map(new CoMapFunction<Integer, String, String>() { //第三个String是两个流合并后输出的类型
                    //处理第一个流的元素
                    @Override
                    public String map1(Integer value) throws Exception {
                        return value + "<";
                    }
                    //处理第二个流的元素
                    @Override
                    public String map2(String value) throws Exception {
                        return value + ">";
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
