package com.tomtop.flink.D_Operator;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author:txf
 * @Date:2022/12/11 10:22
 */
public class Flink06_Shuffle__Rebalance_Rescale {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        env
                .fromElements(1,2,3,4,5,6,7,8)
                .shuffle() //随机分配
                //.rebalance() //平均分配
                //.rescale() //平均分配,完全走的管道,  不经过网络, 效率更高
                .print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
