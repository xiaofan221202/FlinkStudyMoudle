package com.tomtop.flink.D_Operator;

import com.tomtop.flink.A_Bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author:txf
 * @Date:2022/12/11 11:16
 * 对于Reduce
 * 对于一条数据，有分组字段、要聚合字段、和其他非聚合非分组字段，Reduce可以实现非分组非聚合字段是第一条数据的值，也可以是当前的值
 * 注意：Reduce可以同时聚合多个字段， 要聚合的字段类型无所谓，聚合后的类型和聚合前的类型一致
 */
public class Flink12_Reduce {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

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
                .reduce(new ReduceFunction<WaterSensor>() {
                    /*
                    参数解读：
                    参数1：是上次聚合的结果
                    参数2：是这次需要聚合元素
                    过程：当某个key的第一个数据进来时，这个方法不执行。直接输出第一次读进来的数据。从第二个数据开始执行方法、
                    */
                    @Override
                    public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                        //System.out.println(value1.getId() + "  ");//这行代码用来看id的，问题：reduce聚合需不要对id进行操作？不用的。因为只有id是一样的才会被聚合在一起
                        value1.setVc(value1.getVc()+ value2.getVc());//把第一个vc值加上获得第二vc值传到vc中。当前的获得的值的ts是第一个数据的ts的值 和max原理一样
                        return value1;
                        //value2.setVc(value1.getVc() + value2.getVc());//把第一个vc值加上获得第二vc值传到vc中。当前获得的值的ts是当前数据最新的ts的值，和maxBy原理一样
                        //return value2;

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
