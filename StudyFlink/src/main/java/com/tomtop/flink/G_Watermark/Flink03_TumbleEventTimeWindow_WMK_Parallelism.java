package com.tomtop.flink.G_Watermark;

import com.tomtop.flink.A_Bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;

/**
 * @Author:tuxiaofan
 * @Date:2023/1/7 12:09
 * 测试多并行度中水印的情况。
 */
public class Flink03_TumbleEventTimeWindow_WMK_Parallelism {
    public static void main(String[] args) {
        Configuration conf  = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2); //这里测试多并行度的水印生成策略

        env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] data = value.split(",");
                        return new WaterSensor(data[0],Long.valueOf(data[1]),Integer.valueOf(data[2]));
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>(){
                                    @Override
                                    public long extractTimestamp(WaterSensor waterSensor, long timestamp) {
                                        return waterSensor.getTs();
                                    }
                                })
                                //设置如果一个并行度中发现10s没有更新数据(这样就没有更新水印了), 那么水印传递就以其他的为准
                                .withIdleness(Duration.ofSeconds(10))
                )
                .keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        Context context,
                                        Iterable<WaterSensor> element,
                                        Collector<String> collector) throws Exception {
                        ArrayList<WaterSensor> list = new ArrayList<>();
                        for (WaterSensor value : element) {
                            list.add(value);
                        }
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        long processingTime = context.currentProcessingTime();
                        long currentWatermark = context.currentWatermark();
                        collector.collect("输出："+ list+" " + start + ":" + end + ":" + processingTime + ":" + currentWatermark);
                    }
                })
                .print();

        try {
            env.execute("test more Parallelism watermark");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
