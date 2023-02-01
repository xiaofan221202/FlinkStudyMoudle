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
 * @Date:2023/1/10 11:30
 */
public class Flink04_Timer_Practice {
    public static void main(String[] args) throws Exception {
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
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forMonotonousTimestamps()//单调递增策略的水印
                                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>(){
                                    @Override
                                    public long extractTimestamp(WaterSensor waterSensor, long l) {
                                        return waterSensor.getTs();
                                    }
                                })
                )
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    private Long tsTime; //定义时间变化的变量
                    boolean isFirst = true; //判断是否是第一条数据的变量
                    Integer lastVC = 0;  //用于记录每条数据来最后一次的水位值的变量

                    @Override
                    public void onTimer(long timestamp,
                                        OnTimerContext ctx,
                                        Collector<String> out) throws Exception {
                            out.collect("onTImer方法调用:" + ctx.getCurrentKey() + ":" + "连续水位");
                            //注册新的定时器，下一条数据进来时，又会当成新的一条数据使用
                            isFirst = true;
                    }

                    @Override
                    public void processElement(WaterSensor waterSensor,
                                               Context context,
                                               Collector<String> collector) throws Exception {
                        // 如何判断是否第一条数据进来: 定义一个变量(boolean值), 默认是true, 第一个来了之后置为false
                        if(isFirst){
                            tsTime = waterSensor.getTs() + 5000;
                            context.timerService().registerEventTimeTimer(tsTime);
                            System.out.println("第一条数据进来, 注册一个定时器:" + tsTime);
                            //然后把标记改为false
                            isFirst = false;
                         //如果不是第一条数据: 那么就要判断水位是否上升,  对当前的水位值和上一条水位值做对比
                        }else {
                            if (waterSensor.getVc() < lastVC){
                                    //当前水位小于上一条，水位下降，取消定时器
                                context.timerService().deleteEventTimeTimer(tsTime);
                                System.out.println("现在来的数据水位是下降，因此取消定时器");
                                    //接着再注册一个新的定时器
                                tsTime = waterSensor.getTs() + 5000;
                                context.timerService().registerEventTimeTimer(tsTime);
                                System.out.println("注册一个基于事件时间的5s后触发的计时器" + tsTime);
                            }else {
                                   //当前水位是上升的或者是不变的，不处理
                                System.out.println("不处理");
                            }
                        }
                    lastVC = waterSensor .getVc();
                    }
                })
                .print();
        env.execute("定时器作业案例");

    }
}
