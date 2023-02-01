package com.tomtop.flink.G_Watermark;

import com.tomtop.flink.A_Bean.WaterSensor;
import com.tomtop.flink.B_Unit.TomtopUnit;
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
import java.util.List;

/**
 * @Author:tuxiaofan
 * @Date:2023/1/8 23:01
 */
public class Flink04_TumbleEventTimeWindow_WMK_AllowLateness {
    public static void main(String[] args) {
        Configuration conf  = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String s) throws Exception {
                        String[] data = s.split(",");
                        return new WaterSensor(data[0],Long.valueOf(data[1]),Integer.valueOf(data[2]));
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                // 返回值必须是毫秒. 是一个绝对时间
                                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>(){
                                    @Override
                                    public long extractTimestamp(WaterSensor waterSensor, long l) {
                                        return waterSensor.getTs();
                                    }
                                })
//                                .withIdleness(Duration.ofSeconds(10))
                )
                .keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                //这个类的关键点：这里设置允许迟到2s的时间
                .allowedLateness(Time.seconds(2))
/*                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
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
                        collector.collect("输出：" + list + " :" + start + " : " +end + " :" +currentProcessingTime + ":" + currentWatermark);
                    }
                })*/
                .sum("vc")
                .print();

        try {
            env.execute("test 测允许迟到数据");
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
        侧输出流

        如果窗口真正关闭之后, 这是时候来的属于关闭窗口的数据, 会直接进入侧输出流
*/