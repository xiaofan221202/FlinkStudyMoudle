package com.tomtop.flink.E_Sink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.util.Properties;


/**
 * @Author:txf
 * @Date:2022/12/11 19:28
 * 数据是String,可用直接用SimpleStringSchema反序列化器。如果不是String类型就得自定义序列化器。
 */
public class SInk_Kafka_SimpleStringSchema {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        Properties props = new Properties();
        props.setProperty("bootstrap.servers","hadoop162:9092");

        env
                .readTextFile("src/input/word.txt")
                .flatMap(new FlatMapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        for (String word : value.split(" ")) {
                            out.collect(Tuple2.of(word,1L));
                        }
                    }
                })
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> value) throws Exception {
                        return value.f0;
                    }
                })
                .sum(1)
                .map(new MapFunction<Tuple2<String, Long>, String>() {
                    @Override
                    public String map(Tuple2<String, Long> value) throws Exception {
                        return value.f0 + "_" + value.f1;
                    }
                })
                /*
                参数解读：
                "s1":消费主题 ，
                new SimpleStringSchema()反序列化器，
                props : kafka的配置属性
                如果流中数据是String类型可用用SimpleStringSchema()反序列化器。如果不是String类型。则得自定义反序列化器
                */
                .addSink(new FlinkKafkaProducer<String>("s1",new SimpleStringSchema(),props));

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
