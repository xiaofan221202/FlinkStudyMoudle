package com.tomtop.flink.D_Operator;


import com.tomtop.flink.A_Bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author:txf
 * @Date:2022/12/11 10:53
 * 对于Max和Min:
 * 对于一条数据，有分组字段、要聚合字段,和其他非聚合非分组字段，其中聚合字段会随着需要按求最大或求最小的目的来取数据值，但Key就需要根据不同的key来取，非分组非聚合的其他字段取第一条数据的值
 * 注意Sum ,Max,Min , MaxBy , MinBy这些聚合函数只能对其中的一个字段聚合，而且只能只能聚合数字类型。
 */
public class Flink10_Max_Min {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env
                .readTextFile("src/input/sensor.txt")
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] data = value.split(",");
                        return new WaterSensor(
                                data[0],
                                Long.valueOf(data[1]),
                                Integer.valueOf(data[2])
                        );
                    }
                })
                .keyBy(WaterSensor::getId)
                //每来一条数据，如果取最大值，vc值越大就继续更新最大。小就取当前最大值。ts取来的第一条数据(这句话理解就是，非分组字段id,和要聚合的字段是取第一条数据)
                .max("vc")
                //.min("vc")
                .print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
