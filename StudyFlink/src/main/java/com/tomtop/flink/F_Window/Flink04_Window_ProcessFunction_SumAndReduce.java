package com.tomtop.flink.F_Window;

import com.tomtop.flink.A_Bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Author:tuxiaofan
 * @Date:2023/1/5 17:18
 * 基于处理时间的滚动窗口的聚合函数聚合处理
 */
public class Flink04_Window_ProcessFunction_SumAndReduce {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] data = value.split(",");
                        return new WaterSensor(data[0],Long.valueOf(data[1]),Integer.valueOf(data[2]));
                    }
                })
                .keyBy(new KeySelector<WaterSensor, String>() {
                    @Override
                    public String getKey(WaterSensor waterSensor) throws Exception {
                        return  waterSensor.getId();
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                // 注意：聚合是放在窗口后,所以聚合是对每个窗口内的元素进行聚合。如果直接在窗口前进行sum聚合算子操作。那意思就是来一条数据聚合一条数据，无窗口
                //.sum("vc")
                .reduce(new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                        value1.setVc(value1.getVc() + value2.getVc());
                        return value1;
                    }
                })
                .print();

        env.execute("test process function");

    }
}
/*
窗口处理函数:
    开窗的目的: 把多个元素放入到窗口内, 然后对这个窗口内的元素做计算, 比如聚合, 求topN...
增量计算（来一条数据就先计算）
    简单算子
        sum
        max min
        maxBy minBy

    复杂算子
        reduce
        aggregate
全量计算(窗口关闭的时候才进行开始计算)
    process


 */