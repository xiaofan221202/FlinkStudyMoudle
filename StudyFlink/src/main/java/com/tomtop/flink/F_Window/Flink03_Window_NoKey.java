package com.tomtop.flink.F_Window;

import com.tomtop.flink.A_Bean.WaterSensor;
import com.tomtop.flink.B_Unit.TomtopUnit;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * @Author:tuxiaofan
 * @Date:2023/1/5 16:43
 */
public class Flink03_Window_NoKey {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] data = value.split(",");
                        return new WaterSensor(data[0],Long.valueOf(data[1]),Integer.valueOf(data[2]));
                    }
                })
                // 没有keyBy窗口, 要求所有的元素进入同一个窗口,所以要求窗口处理函数process的并行度必须是1(这里可以通过webui界面看，会发现map和sink的并行度为2，而socket和process为1)
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new ProcessAllWindowFunction<WaterSensor, String, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<WaterSensor> iterable, Collector<String> collector) throws Exception {
                        //迭代器转list
                        List<WaterSensor> list = TomtopUnit.toList(iterable);
                        //窗口开始关闭时间
                        String startTime = TomtopUnit.toDateTime(context.window().getStart());
                        String endTime = TomtopUnit.toDateTime(context.window().getEnd());
                        //输出
                        collector.collect("输出：" + list + " " + startTime + " " + endTime);

                    }
                })
                .print();

        env.execute("test nokey window");
    }
}
