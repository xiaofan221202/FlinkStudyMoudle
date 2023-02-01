package com.tomtop.flink.G_Watermark;

import com.tomtop.flink.A_Bean.WaterSensor;

import com.tomtop.flink.B_Unit.TomtopUnit;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.api.java.functions.KeySelector;
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
 * @Date:2023/1/6 17:25
 */
public class Flink01_TumbleEventTimeWindow_WMK {
    public static void main(String[] args) {
        Configuration conf  = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1); //测试watermark，并行度必须设置为1

        env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] data = value.split(",");
                        return new WaterSensor(data[0],Long.valueOf(data[1]),Integer.valueOf(data[2]));
                    }
                })
                //本类的关键在这里：设置水印和提取时间戳的策略
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3)) //设置3秒乱序
                                .withTimestampAssigner((element, timeStamp) -> element.getTs()) //提取事件时间时间戳，返回时间必须是毫秒值，是一个绝对时间，就是自然界的时间。
                )
                .keyBy(new KeySelector<WaterSensor, String>() {
                    @Override
                    public String getKey(WaterSensor waterSensor) throws Exception {
                        return waterSensor.getId();
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        Context context,
                                        Iterable<WaterSensor> element,
                                        Collector<String> collector) throws Exception {
                        //迭代器转集合，方便处理元素
                        //List<WaterSensor> list = TomtopUnit.toList(element);
                        ArrayList<WaterSensor> list = new ArrayList<>();
                        for (WaterSensor value : element) {
                            list.add(value);
                        }
                        //获取窗口信息
                        String startTime = TomtopUnit.toDateTime(context.window().getStart());
                        long endTime = context.window().getEnd();
                        //输出
                        collector.collect("输出："+ key + " " + startTime + " " + endTime + " " + list);

                    }
                })
                .print();

        try {
            env.execute("test Tumble Process Window watermark and timestamps");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
