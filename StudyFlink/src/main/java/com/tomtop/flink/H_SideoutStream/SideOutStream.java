package com.tomtop.flink.H_SideoutStream;

import com.alibaba.fastjson.JSON;
import com.tomtop.flink.A_Bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author:tuxiaofan
 * @Date:2023/1/8 23:45
 * 侧输出流
 */
public class SideOutStream {
    public static void main(String[] args) {
        Configuration conf  = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        /*
        * 需求：把输出的数据如果是key1的当成主流，key2的当成侧输出流，其他key*的当成其他侧输出流。
        * */

        SingleOutputStreamOperator<String> souece = env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String s) throws Exception {
                        String[] data = s.split(",");
                        return new WaterSensor(data[0], Long.valueOf(data[1]), Integer.valueOf(data[2]));
                    }
                })
                .process(new ProcessFunction<WaterSensor, String>() {
                    @Override
                    public void processElement(WaterSensor waterSensor, //输入
                                               Context context, //上下文
                                               Collector<String> collector) throws Exception {//输出
                        if ("key1".equals(waterSensor.getId())) {
                            collector.collect("这是主流的数据：" + waterSensor.toString());
                        } else if ("key2".equals(waterSensor.getId())) {
                            context.output(new OutputTag<String>("侧输出流"){}, JSON.toJSONString(waterSensor));
                        } else {
                            context.output(new OutputTag<WaterSensor>("其他侧输出流"){}, waterSensor);
                        }

                    }
                });

        souece.print("主流");
        souece.getSideOutput(new OutputTag<String>("侧2"){} )
                .print("侧输出流2");
        souece.getSideOutput(new OutputTag<WaterSensor>("other"){})
                .print("其他");

        try {
            env.execute("测试侧输出流");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
