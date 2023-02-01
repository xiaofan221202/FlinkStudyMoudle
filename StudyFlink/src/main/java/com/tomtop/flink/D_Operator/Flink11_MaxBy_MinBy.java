package com.tomtop.flink.D_Operator;


import com.tomtop.flink.A_Bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author:txf
 * @Date:2022/12/11 11:01
 * 对于MaxBy和MinBy:
 * 对于一条数据，有分组字段、要聚合字段,和其他非聚合非分组字段，其中聚合字段会随着需要按求最大或求最小的目的来取数据值，但Key就需要根据不同的key来取，非分组非聚合的其他字段不按第一条数据的值取，根据聚合字段求最大或最小值的那条数据中取
 * 注意Sum ,Max,Min , MaxBy , MinBy这些聚合函数只能对其中的一个字段聚合，而且只能只能聚合数字类型。
 */
public class Flink11_MaxBy_MinBy {
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
                //求最大值时，每来一条数据，vc值越大就继续更新最大。小就取当前最大值。后面的每个ts字段不取第一个来数据的值(这句话理解就是，除了非分组字段id,和要聚合的字段vc。其他字段ts会随着最大或最小来取。谁大就取谁的ts)
                .maxBy("vc", false)  // 不取第一个
                // .minBy("vc",false)
                .print();



        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
