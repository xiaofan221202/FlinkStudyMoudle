package com.tomtop.flink.F_Window;

import com.tomtop.flink.A_Bean.WaterSensor;
import com.tomtop.flink.B_Unit.TomtopUnit;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * @Author:tuxiaofan
 * @Date:2023/1/5 15:15
 * 基于时间（处理时间）的窗口：滑动，滚动，会话。
 */
public class Flink01_Window_Tumble_Slide_Session {
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
                        return new WaterSensor(data[0],Long.valueOf(data[1]),Integer.valueOf(data[2])) ;
                    }
                })
                .keyBy(new KeySelector<WaterSensor, String>() {
                    @Override
                    public String getKey(WaterSensor waterSensor) throws Exception {
                        return waterSensor.getId();
                    }
                })
                //定义窗口
                //.window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(2))) //窗口长度为5s，滑动步长3s的基于处理时间的滑动窗口
                //.window(ProcessingTimeSessionWindows.withGap(Time.seconds(3)))//窗口大小3s的基于处理时间的会话窗口
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                //窗口处理
                /*
                窗口处理函数：ProcessWindowFunction
                WaterSensor:输入的数据类型
                String:输出数据类型
                String:key的类型
                TimeWindow :窗口类型
                */
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                /*
                处理窗口中元素的方法：当窗口关闭时执行一次
                key:属于那个key的窗口
                context:上下文对象，可以获取到窗口信息
                elements:窗口触发计算的时候存储着窗口类所有的元素
                collector：输出的数据流
                */
                    @Override
                    public void process(String key,
                                        Context context,
                                        Iterable<WaterSensor> elements,
                                        Collector<String> collector) throws Exception {

                        //Iterable很多操作不能做，比如对元素进行排序，因此这里一般把迭代器中元素转成一个list集合后再操作
                        ArrayList<WaterSensor> list = new ArrayList<>();
                        for (WaterSensor element : elements) {
                            list.add(element);
                        }

                        //获取窗口开始时间(yyyy-MM-dd HH:mm:ss样式的字符串
                        String startTime = TomtopUnit.toDateTime(context.window().getStart());
                        //获取窗口结束时间(yyyy-MM-dd HH:mm:ss样式的字符串
                        String endTime= TomtopUnit.toDateTime(context.window().getEnd());
                        //输出（数据如何处理）
                        collector.collect("输出数据："+key+" "+startTime+" "+endTime+" "+list);
                    }
                })
                .print();

        try {
            env.execute("test window");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


/*
窗口
   一、基于时间的窗口
        1、属性：具有开始时间和结束时间
        2、理解：可以把窗口理解为一个桶。在一定时间内存放所有的数据，在窗口时间结束，输出计算的结果
        3、分类：
            滚动窗口；每隔几秒完成窗口计算；窗口之间没有间隙也没有重叠
            滑动窗口：有窗口时间，和滑动步长。窗口时间结束就输出计算结果。但是每个窗口开始时间为滑动步长开始的时间。
                     窗口之间有间隙也可能有重叠
                     sparkStreaming和flink中的滑动窗口的区别：sparkStreaming每个几秒打印一个时间戳。但flink没有数据是不打印
            sessin窗口：超过gap时间没有输入数据，窗口就关闭。

   二、基于个数的窗口
        1、属性：看进入数据的个数
        2、理解：可以把窗口理解为一个桶。存储对应个数的数据
        3、分类：
            滚动窗口：根据数据个数进行滚动
            滑动窗口：根据数量进行滑动。

   三、窗口和key的关系
        1、问题：在关闭窗口时，不同的key的关闭时间是否是一样的？
            答：滚动和滑动是同时关闭。session不会同时关闭。不不同key的开始时间和关闭时间，一般不一样。
        2、keyBy和窗口的关系：
            有keyBy的窗口：用window方法：不同分区有各自的窗口.并行度没有要求
	        没有keyBy的加窗口：用windowAll方法 :所有的元素必须进入同一个窗口，所以窗口处理的函数的并行度必须为1

	四、窗口处理函数：
	    1、开窗的目的: 把多个元素放入到窗口内, 然后对这个窗口内的元素做计算, 比如聚合, 求topN...
        2、增量计算（来一条数据就先计算）
            简单算子
                sum
                max min
                maxBy minBy
            复杂算子
                reduce
                aggregate
        3、全量计算(窗口关闭的时候才进行开始计算)
            process
        4、注意：聚合函数是放在窗口函数后,所以聚合是对每个窗口内的元素进行聚合
*/