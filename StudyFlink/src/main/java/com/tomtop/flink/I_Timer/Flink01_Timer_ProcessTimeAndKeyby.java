package com.tomtop.flink.I_Timer;

import com.tomtop.flink.A_Bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author:tuxiaofan
 * @Date:2023/1/9 17:23
 */
public class Flink01_Timer_ProcessTimeAndKeyby {
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
                .keyBy(WaterSensor::getId)
                // 需求：如果vc的值大于 20 , 就定义一个 5s后触发的基于处理时间的定时器;当vc的值小于10，就删除定时器
                .process(new KeyedProcessFunction<String, WaterSensor, String>() { //泛型<key,输入,输出>
                    private long ts ;

                    //如果要加定时器，就需要重写这个方法；当定时器触发的时候, 会调用这个方法；一个定时器只会触发一次。这里是定时器触发才会调用。
                    @Override
                    public void onTimer(long timestamp,// 定时器的时间
                                        OnTimerContext ctx,
                                        Collector<String> out) throws Exception {
                        System.out.println("Flink01_Timer_ProcessTimeAndKeyby.onTimer");
                        String currentKey = ctx.getCurrentKey();
                        Long timestamp1 = ctx.timestamp();
                        out.collect("onTimer调用了，定时器被触发了：" + currentKey + ": "+ timestamp1 + ":" + "如果水位超出20，那么过5秒后就会触发，如果水位是低于10的，那么定时器会被删除，5秒不会触发报警");
                    }
                    //这个方法就是用来申明定时器或者是删除定时器，每来一个元素方法会调用一次，
                    @Override
                    public void processElement(WaterSensor waterSensor, //输入
                                               Context context, //上下文
                                               Collector<String> collector) throws Exception {//输出
                        System.out.println("Flink01_Timer_ProcessTimeAndKeyby.processElement");
                        if (waterSensor.getVc()>20){
                            //时间要是绝对时间,当前时间戳
                            ts = System.currentTimeMillis() + 5000;
                            context.timerService().registerProcessingTimeTimer(ts); //登记处理时间的计时器
                            String currentKey1 = context.getCurrentKey();
                            System.out.println(currentKey1+":"+"看看定时器创建好没"+ ":"+ts);
                            collector.collect("元素处理后的输出" + ":" + currentKey1+ " : " + ts + "");

                        }else if (waterSensor.getVc() < 10 ){
                            context.timerService().registerProcessingTimeTimer(ts);//删除处理时间的计时器
                            String currentKey2 = context.getCurrentKey();
                            System.out.println(currentKey2+":"+"看看定时器删了么"+ ":"+ts);
                            collector.collect("元素处理后的输出:"+ ":" + currentKey2 + ":" + ts +"");
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
