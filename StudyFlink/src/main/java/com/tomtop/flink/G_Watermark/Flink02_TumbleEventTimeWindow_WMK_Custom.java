package com.tomtop.flink.G_Watermark;

import com.tomtop.flink.A_Bean.WaterSensor;
import com.tomtop.flink.B_Unit.TomtopUnit;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * @Author:tuxiaofan
 * @Date:2023/1/7 10:59
 */
public class Flink02_TumbleEventTimeWindow_WMK_Custom {
    public static void main(String[] args) {
        Configuration conf  = new Configuration();
        conf.setInteger("rest.prot", 2000);
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
                        new WatermarkStrategy<WaterSensor>() {
                            @Override
                            public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                                return new MyWaterMark();
                            }
                        }
                        .withTimestampAssigner((element,ts) -> element.getTs())
                )
                .keyBy(new KeySelector<WaterSensor, String>() {
                    @Override
                    public String getKey(WaterSensor waterSensor) throws Exception {
                        return waterSensor.getId();
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        Context context,
                                        Iterable<WaterSensor> element,
                                        Collector<String> collector) throws Exception {
                        List<WaterSensor> list = TomtopUnit.toList(element);
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        collector.collect("?????????" + key + ":" + start + ":" + end + ":" + list);
                    }
                })
                .print();

        try {
            env.execute("test watermark custom ");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    //?????????watermark
    private static class MyWaterMark implements WatermarkGenerator<WaterSensor> {
        //????????????????????????
        long maxTs = Long.MIN_VALUE + 3000 + 1;


        //onEvent(T t, long l, org.apache.flink.api.common.eventtime.WatermarkOutput watermarkOutput);
        //onEvent???????????????????????????????????????????????? ????????????WaterSensor:?????????????????? long???????????????????????????????????? ??? WatermarkOutput??????????????????
        @Override
        public void onEvent(WaterSensor waterSensor, long l, WatermarkOutput watermarkOutput) {
            // ?????????????????????
            maxTs = Math.max(maxTs, l);
            // ??????????????????/?????????
            watermarkOutput.emitWatermark(new Watermark(maxTs - 3000 - 1));
        }

        // ??????????????????????????????:?????????200ms
    @Override
    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
        System.out.println("MyWaterMark.onPeriodicEmit");
    }
}
}
