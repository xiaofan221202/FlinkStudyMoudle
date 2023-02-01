package com.tomtop.flink.D_Operator;


import com.tomtop.flink.A_Bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author:txf
 * @Date:2022/12/11 18:15
 * process这个算子，可以在keyBy之后使用。也可以不keyBy后使用
 * 不keyBy之后使用:不可以使用算子状态，后面的知识点：不然会报错
 * keyBy之后使用：方法中的计算。例如：里面的求和计算和key无关。
 * process这个算子的KeyedProcessFunction这个方法中的计算和key无关，和并行度相关。
 */
public class Flink14_Process_Key {

        public static void main(String[] args) {
            Configuration conf = new Configuration();
            conf.setInteger("rest.port", 2000);
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
            env.setParallelism(2);

            //需求：统计每个传感器的水位和
            env
                    .readTextFile("src/input/sensor.txt")
                    .map(new MapFunction<String, WaterSensor>() {
                        @Override
                        public WaterSensor map(String value) throws Exception {
                            String[] data = value.split(",");
                            return new WaterSensor(
                                    data[0],
                                    Long.valueOf(data[1]),
                                    Integer.valueOf(data[2])
                            );

                        }
                    })
                    .keyBy(WaterSensor::getId)
                    .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                        int sum = 0; //定义状态，不分key ,和并行度有关。一个并行度。所有的数据在这个并行度相加，和id无关
                        @Override
                        public void processElement(WaterSensor value,
                                                   Context ctx,
                                                   Collector<String> out) throws Exception {
                            sum += value.getVc();
                            out.collect(ctx.getCurrentKey()+"的水位和："+ sum);
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
