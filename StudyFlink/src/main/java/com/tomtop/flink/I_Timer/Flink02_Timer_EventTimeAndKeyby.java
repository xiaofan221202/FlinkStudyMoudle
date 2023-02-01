package com.tomtop.flink.I_Timer;

import com.tomtop.flink.A_Bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


/**
 * @Author:tuxiaofan
 * @Date:2023/1/10 9:53
 */
public class Flink02_Timer_EventTimeAndKeyby {
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
                                .<WaterSensor>forMonotonousTimestamps() //设置单调递增的水印
                                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>(){
                                    @Override
                                    public long extractTimestamp(WaterSensor waterSensor, long l) {
                                        return waterSensor.getTs();
                                    }
                                })
                )
                .keyBy(WaterSensor::getId)
                //需求：定义一个定时器，当vc值大于20，基于事件时间5秒后触发。当vc小于10，删除掉定时器。
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    private Long ts;

                    @Override
                    public void onTimer(long timestamp, //定时器时间
                                        OnTimerContext ctx, //上下文
                                        Collector<String> out) throws Exception {//输出
                        Long timestamp1 = ctx.timestamp();
                        String currentKey = ctx.getCurrentKey();
                        System.out.println(timestamp+ " :"  + timestamp1 + " :" +currentKey + " : " + "这个方法onTimer被调用了一次");
                    }
                    @Override
                    public void processElement(WaterSensor waterSensor,
                                               Context context,
                                               Collector<String> collector) throws Exception {
                        if(waterSensor.getVc() > 20){
                            // ts = System.currentTimeMillis() + 5000;  //这里时间很关键。这里用到水印和事件时间，那么就不能像处理时间那样用系统时间
                            ts = waterSensor.getTs() + 5000;
                            context.timerService().registerEventTimeTimer(ts);
                            collector.collect("当vc大于20元素处理后输出");
                            System.out.println("说明这里运行了，设置了一个基于事件处理的定时器:" + context.getCurrentKey() +":" + context.timestamp());
                        }else if (waterSensor.getVc()< 10){
                            context.timerService().deleteEventTimeTimer(ts);
                            collector.collect("当vc小于10元素处理后输出");
                            System.out.println("说明这里运行了，删除了一个基于事件处理的定时器:" + context.getCurrentKey() +":" + context.timestamp());
                        }
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
