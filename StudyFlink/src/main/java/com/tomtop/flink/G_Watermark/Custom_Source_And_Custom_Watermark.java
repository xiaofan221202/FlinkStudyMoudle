package com.tomtop.flink.G_Watermark;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.UUID;

/**
 * @Author:tuxiaofan
 * @Date:2023/1/7 11:40
 */
public class Custom_Source_And_Custom_Watermark {
    public static void main(String[] args){

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置周期性水印的间隔
        env.getConfig().setAutoWatermarkInterval(5000L);
        DataStream<Tuple2<String,Long>> dataStream = env.addSource(new MySource());

        DataStream<Tuple2<String,Long>> withTimestampsAndWatermarks = dataStream.assignTimestampsAndWatermarks(
                new WatermarkStrategy<Tuple2<String,Long>>(){
                    @Override
                    public WatermarkGenerator<Tuple2<String,Long>> createWatermarkGenerator(
                            WatermarkGeneratorSupplier.Context context){
                        //返回自定义的Watermark
                        return new WatermarkGenerator<Tuple2<String,Long>>(){
                            private long maxTimestamp;
                            private long delay = 3000;

                            @Override
                            public void onEvent(Tuple2<String,Long> event, long eventTimestamp, WatermarkOutput output){
                                //计算最大时间戳
                                maxTimestamp = Math.max(maxTimestamp, event.f1);
                            }

                            //这个方法每200ms执行一次
                            @Override
                            public void onPeriodicEmit(WatermarkOutput output){
                                // 间隙性的水印/打点式
                                output.emitWatermark(new Watermark(maxTimestamp - delay));
                            }
                        };
                    }
                });

//使用内置的水印生成器
//		DataStream<Tuple2<String,Long>> withTimestampsAndWatermarks = dataStream.assignTimestampsAndWatermarks(
//				WatermarkStrategy
//						.<Tuple2<String,Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
//						.withTimestampAssigner((event, timestamp)->event.f1));

        withTimestampsAndWatermarks
                .process(new ProcessFunction<Tuple2<String,Long>,Object>(){
                        @Override
                        public void processElement(
                                Tuple2<String,Long> value, Context ctx, Collector<Object> out) throws Exception{
                            long w = ctx.timerService().currentWatermark();
                            System.out.println(" 水印 ： " + w + "  water  date " + new Date(w) + " now " + new Date(value.f1));
                        }
                    });

        try {
            env.execute();
        } catch (Exception e){
            e.printStackTrace();
        }
    }


    //自定义source
    public static class MySource implements SourceFunction<Tuple2<String,Long>> {
        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<Tuple2<String,Long>> ctx) throws Exception{
            while (isRunning){
                Thread.sleep(1000);
                //订单id
                String orderid = UUID.randomUUID().toString();
                //订单完成时间
                long orderFinishTime = System.currentTimeMillis();
                ctx.collect(Tuple2.of(orderid, orderFinishTime));
            }
        }

        @Override
        public void cancel(){
            isRunning = false;
        }
    }
}
