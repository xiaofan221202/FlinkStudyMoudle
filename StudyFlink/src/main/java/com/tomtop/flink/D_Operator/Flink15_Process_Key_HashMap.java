package com.tomtop.flink.D_Operator;


import com.tomtop.flink.A_Bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author:txf
 * @Date:2022/12/11 18:23
 * process这个算子，可以在keyBy之后使用。也可以不keyBy后使用
 * 不keyBy之后使用:不可以使用算子状态，后面的知识点：不然会报错
 * keyBy之后使用：方法中的计算。例如：里面的求和计算和key无关。
 * process这个算子的KeyedProcessFunction这个方法中的计算和key无关，和并行度相关。
 */
public class Flink15_Process_Key_HashMap {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(3);

        //需求：统计每个传感器的水位和
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
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    //定义HashMap集合。来收集流中的数据。封装成HashMap集合
                    Map<String, Integer> IdToSum=  new HashMap<>();

                    @Override
                    public void processElement(WaterSensor value,
                                               Context ctx,
                                               Collector<String> out) throws Exception {
                        String key = ctx.getCurrentKey(); //keyBy分组后获取当前的key的值
                        Integer sum = IdToSum.getOrDefault(key,0);// 方法是从map只取值, 如果存在就返回, 否则返回0  其实就第一次调用方法时里面的值为0
                        sum += value.getVc();//从HashMap集合中的第一个值开始。其实就是0.因为没有。然后累加当前的vc值进行求和
                        IdToSum.put(key,sum);  //当前的值通过k-v方式封账到HashMap集合中
                        out.collect(ctx.getCurrentKey()+":的水位值和为："+ sum);

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
