package com.tomtop.flink.G_Watermark;

import com.tomtop.flink.A_Bean.WaterSensor;
import com.tomtop.flink.B_Unit.TomtopUnit;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;

/**
 * @Author:tuxiaofan
 * @Date:2023/1/8 23:19
 */
public class Flink05_TumbleEventTimeWindow_WMK_Sideout {
    public static void main(String[] args) {
        Configuration conf  = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        SingleOutputStreamOperator<String> source = env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String s) throws Exception {
                        String[] data = s.split(",");
                        return new WaterSensor(data[0], Long.valueOf(data[1]), Integer.valueOf(data[2]));
                }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                                    @Override
                                    public long extractTimestamp(WaterSensor waterSensor, long l) {
                                        return waterSensor.getTs();
                                    }
                                })
                )
                .keyBy(new KeySelector<WaterSensor, String>() {
                    @Override
                    public String getKey(WaterSensor waterSensor) throws Exception {
                        return waterSensor.getId();
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                //本类的关键：把真正迟到的迟到(窗口之后进来的数据)的数据写入到侧输出流中;做成匿名内部类的方式,来保存泛型到运行时
                .sideOutputLateData(new OutputTag<WaterSensor>("late") {
                })
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        Context context,
                                        Iterable<WaterSensor> element,
                                        Collector<String> collector) throws Exception {
                        List<WaterSensor> list = TomtopUnit.toList(element);
                        String start = TomtopUnit.toDateTime(context.window().getStart());
                        String end = TomtopUnit.toDateTime(context.window().getEnd());
                        long currentWatermark = context.currentWatermark();
                        long currentProcessingTime = context.currentProcessingTime();
                        collector.collect("输出：" + list + " :" + start + " : " + end + " :" + currentProcessingTime + ":" + currentWatermark);
                    }
                });

        source.print("正常数据");
        source.getSideOutput(new OutputTag<WaterSensor>("late"){}).print("迟到数据");

        try {
            env.execute("测试侧输出流");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

/*
 1. 解决数据迟到问题的第一重保证
        事件时间+水印

2. 解决数据迟到的第二重保证
        允许迟到

        当到了窗口关闭时间, 窗口先不关闭, 只是对窗口内的元素进行计算.
        然后允许迟到期间, 如果有属于这个窗口的数据来了, 则来一条就计算一次.
        超过了允许迟到时间, 窗口才真正的关闭!!!!

3. 解决数据迟到的第三重保证
    测输出路
        如果窗口真正关闭之后, 这是时候来的属于关闭窗口的数据, 会直接进入侧输出流
 */