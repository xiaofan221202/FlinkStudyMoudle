package com.tomtop.flink.F_Window;

import com.tomtop.flink.A_Bean.WaterSensor;
import com.tomtop.flink.B_Unit.TomtopUnit;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author:tuxiaofan
 * @Date:2023/1/6 11:24
 */
public class Flink06_Window_ProcessFunction_ReduceAndProcess {
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
                        return new WaterSensor(data[0],Long.valueOf(data[1]),Integer.valueOf(data[2]));
                    }
                })
                .keyBy(new KeySelector<WaterSensor, String>() {
                    @Override
                    public String getKey(WaterSensor waterSensor) throws Exception {
                        return waterSensor.getId();
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<WaterSensor>() {
                                @Override
                                public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                                    value1.setVc(value1.getVc()+value2.getVc());
                                    return value1;
                                 }
                        },
                        //本类的关键在这里：这是给开窗后进行开窗reduce的聚合处理。在reduce()方法中，加了个参数new ProcessWindowFunction，是为了补充窗口信息（因为在对数据聚合处理后，可能还想知道关于窗口的一些信息，比如开始结束时间等）。
                        new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {//<输入类型，输出类型，key类型,窗口类型>
                            @Override
                            //process(key , 上下文，元素存在迭代器，输出)
                            public void process(String key,
                                                Context context,
                                                // Iterable这个集合存储的值, 是聚合函数最终的结果，所以说, 这个集合中有且仅有一个元素, 就是前面聚合的最终结果，这里可以对这个结果最再加工: 补充点窗口信息
                                                Iterable<WaterSensor> element,
                                                Collector<String> collector) throws Exception {
                                System.out.println("这里窗口处理函数是在窗口关闭时执行");
                                WaterSensor watersensor = element.iterator().next();
                                collector.collect("输出："+ watersensor + " " + key + " " + TomtopUnit.toDateTime(context.window().getStart()) + " "+context.window().getEnd());
                            }
                        }
                        )
                .print();

        try {
            env.execute("test window process function reduce and process");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
