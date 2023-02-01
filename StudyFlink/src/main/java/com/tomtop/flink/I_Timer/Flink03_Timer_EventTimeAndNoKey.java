package com.tomtop.flink.I_Timer;

import com.tomtop.flink.A_Bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author:tuxiaofan
 * @Date:2023/1/10 10:57
 */
public class Flink03_Timer_EventTimeAndNoKey {
    public static void main(String[] args) throws Exception {
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
                        return  new WaterSensor(data[0],Long.valueOf(data[1]),Integer.valueOf(data[2]));
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forMonotonousTimestamps() //单调递增的水印
                                .withTimestampAssigner((ws, ts) -> ws.getTs())
                )
                .process(new ProcessFunction<WaterSensor, String>() {
                    private  Long ts ;
/*                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                    }*/

                    @Override
                    public void processElement(WaterSensor waterSensor, //输入
                                               Context context,//上下文
                                               Collector<String> collector) throws Exception {//输出
                        ts = waterSensor.getTs() + 5000 ;
                        context.timerService().registerEventTimeTimer(ts);
                        System.out.println(context.timestamp() + ":" + ts + ":" + "设置一个基于事件时间5s处理的计时器");
                    }
                })
                .print();

        env.execute();

    }
}
