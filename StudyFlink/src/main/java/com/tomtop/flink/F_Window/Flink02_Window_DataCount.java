package com.tomtop.flink.F_Window;

import com.tomtop.flink.A_Bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * @Author:tuxiaofan
 * @Date:2023/1/5 16:17
 * 基于数量的窗口：滑动和滚动
 */
public class Flink02_Window_DataCount {
    public static void main(String[] args) {
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
                        return new WaterSensor(data[0], Long.valueOf(data[1]), Integer.valueOf(data[2]));

                    }
                })
                .keyBy(new KeySelector<WaterSensor, String>() {
                    @Override
                    public String getKey(WaterSensor waterSensor) throws Exception {
                        return waterSensor.getId();
                    }
                })
                //基于数量的滚动窗口，每个窗口必须有三个元素才输出
                //.countWindow(3)
                //基于数量的滑动窗口，每个窗口必须来2个元素才输出，窗口最多存储3个元素，从最后一个元素向前找3个元素就是这个窗口
                .countWindow(3,2)
                .process(new ProcessWindowFunction<WaterSensor, String, String, GlobalWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<WaterSensor> elements, Collector<String> collector) throws Exception {
                        ArrayList<WaterSensor> list = new ArrayList<>();
                        for (WaterSensor element : elements) {
                            list.add(element);
                        }
                        collector.collect("输出："+ key + " " + list);

                    }
                })
                .print();

        try {
            env.execute("test count window");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
