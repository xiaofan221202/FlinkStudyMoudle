package com.tomtop.flink.D_Operator;


import com.tomtop.flink.A_Bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author:txf
 * @Date:2022/12/11 18:08
 * process这个算子，可以在keyBy之后使用。也可以不keyBy后使用
 * 不keyBy之后使用:不可以使用算子状态，后面的知识点：不然会报错
 * keyBy之后使用：方法中的计算。例如：里面的求和计算和key无关。
 * process这个算子的KeyedProcessFunction这个方法中的计算和key无关，和并行度相关。
 */
public class Flink13_Process_NoKey {

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

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
                .process(new ProcessFunction<WaterSensor, String>() {


                        /*
                        //补充：这里是后面学习的到状态。状态必须要和keyBy一起使用。测试用写的代码，这里代码放出来会报错：
                        //NullPointerException: Keyed state can only be used on a 'keyed stream', i.e., after a 'keyBy()' operation.
                        private ValueState<String> a;
                        @Override
                        public void open(Configuration parameters) throws Exception {
                            //这一步是获取值的状态。把a 提升成成员变量。后面会详细讲到
                            a = getRuntimeContext().getState(new ValueStateDescriptor<String>("a", String.class));
                        }*/


                    /*
                    方法解读：这个processElement方法是用来处理流中数据的
                    参数解读：参数1：就是要处理的数据元素，参数2：是上下文对象，参数3：是输出的数据
                    */
                    int sum = 0;//定义求和的初始状态。也就是刚开始水位的求和值为0
                    @Override
                    public void processElement(WaterSensor value,
                                               Context ctx,
                                               Collector<String> out) throws Exception {
                        sum += value.getVc();  //不断获取流中的值进行累积求和
                        out.collect("水位和为："+ sum); //把结果进行输出

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
