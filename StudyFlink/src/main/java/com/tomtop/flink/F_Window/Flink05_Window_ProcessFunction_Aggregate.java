package com.tomtop.flink.F_Window;

import com.tomtop.flink.A_Bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Author:tuxiaofan
 * @Date:2023/1/5 18:26
 */
public class Flink05_Window_ProcessFunction_Aggregate {
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
                .aggregate(new AggregateFunction<WaterSensor, Avg, Double>() {
                    /*参数解读 AggregateFunction <IN, ACC, OUT>
                    In 输入类型
                    ACC 累加器
                    Out 输出类型
                    重写四个方法
                    */

                    //初始化一个累加器。方法返回一个Avg类型累加器。因此只需new就行；这个方法当窗口内的第一条元素来时只触发一次
                    @Override
                    public Avg createAccumulator() {
                        //快捷键 soutm 用于检查方法运行的次数
                        System.out.println("Flink05_Window_ProcessFunction_Aggregate.createAccumulator");
                        return new Avg();
                    }
                    //真正累加器的方法：方法每来一条元素执行一次
                    @Override
                    public Avg add(WaterSensor waterSensor, Avg acc) {
                        System.out.println("Flink05_Window_ProcessFunction_Aggregate.add");
                         acc.sum += waterSensor.getVc(); //每来一条waterSensor对象中的vc的值就加上。求总和
                         acc.count ++;  //求总数
                        return acc;
                    }
                    //返回最终结果的方法,窗口关闭时触发一次
                    @Override
                    public Double getResult(Avg acc) {
                        System.out.println("Flink05_Window_ProcessFunction_Aggregate.getResult");
                        return  (double)(acc.sum / acc.count);
                    }
                    //合并累加器的方法：只有窗口是 session窗口的时候才有效,其他窗口不会触发
                    @Override
                    public Avg merge(Avg acc1, Avg acc2) {
                        System.out.println("Flink05_Window_ProcessFunction_Aggregate.merge");
                        acc1.sum += acc2.sum;
                        acc1.count += acc2.count;
                        return acc1;
                    }
                })
                .print();

        try {
            env.execute("test window function aggregate");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //用于求平均值的类。这里是自定义累计器（为啥要有累加器。因为在复杂的聚合函数aggregate使用中，并不是来一条就算一条。可能是要求平均值，那么就需要多个值。）
    private static class Avg {
        public Integer sum = 0;
        public Long count = 0L;
    }
}
