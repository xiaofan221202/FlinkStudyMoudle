package com.tomtop.flink.D_Operator;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author:txf
 * @Date:2022/12/11 10:00
 */
public class Flink03_Filter {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        //需求：返回list集合中偶数的值 ;因为filter是一进一出，这里的泛型就可以自动推断确定
        env.fromElements(1,2,3,4,5,6,7)
                .filter(new FilterFunction<Integer>() {
                    @Override
                    public boolean filter(Integer value) throws Exception {
                        //return 2 % 2 ==0;  //为true 返回list中所有值
                       return value % 2 ==0; //返回偶数，偶数为true
                    }
                }).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
