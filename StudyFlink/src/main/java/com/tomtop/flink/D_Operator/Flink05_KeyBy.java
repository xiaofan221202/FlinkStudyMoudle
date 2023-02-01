package com.tomtop.flink.D_Operator;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
/**
 * @Author:txf
 * @Date:2022/12/11 10:11
 */
public class Flink05_KeyBy {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        // 分别计算奇数的和 与 偶数的和；要聚合: 必须先分组(keyBy), 聚合的时候就是每组内实现聚合
        env.fromElements(1,2,3,4,5,6,7)
                .keyBy(new KeySelector<Integer, String>() {
                    @Override
                    public String getKey(Integer value) throws Exception {
                        return value % 2 ==0 ? "偶数" :"奇数";
                    }
                })
                //把值进行聚合。这里写0因为这里只有一个值,即是Key又是值，下标是0
                .sum(0)
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
