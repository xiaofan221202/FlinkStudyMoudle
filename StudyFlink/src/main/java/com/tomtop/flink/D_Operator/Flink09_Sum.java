package com.tomtop.flink.D_Operator;

import com.tomtop.flink.A_Bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author:txf
 * @Date:2022/12/11 10:46
 */
public class Flink09_Sum {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        //需求：统计每个传感器(watersensor)的水位和
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
                .keyBy(new KeySelector<WaterSensor, String>() {
                    @Override
                    public String getKey(WaterSensor value) throws Exception {
                        return value.getId();
                    }
                })
                .sum("vc")
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
