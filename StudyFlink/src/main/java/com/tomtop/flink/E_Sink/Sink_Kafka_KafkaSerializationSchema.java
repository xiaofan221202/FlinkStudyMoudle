package com.tomtop.flink.E_Sink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic.AT_LEAST_ONCE;

/**
 * @Author:txf
 * @Date:2022/12/11 19:29
 * 数据是String,可用直接用SimpleStringSchema反序列化器。如果不是String类型就得自定义序列化器。
 */
public class Sink_Kafka_KafkaSerializationSchema {
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
                //如果流中的数据不是字符串类型：则需要自己自定义序列化器 ：现在流中数据聚合后数据类型是二元元组类型。所有需要自定义序列化器。
                /*
                4个参数解读：选的构造器是4个参数的
                "default":消费主题 ，
                new KafkaSerializationSchema<>自定义序列化器，
                props : kafka的配置属性
                AT_LEAST_ONCE：语义。选择至少一次
                */
                .addSink(new FlinkKafkaProducer<Tuple2<String,Long>>(
                        "default",
                        new KafkaSerializationSchema<Tuple2<String, Long>>() {
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, Long> value, @Nullable Long timestamp) {
                                String msg = value.f0 + "_" + value.f1;
                                return new ProducerRecord<>("s1", msg.getBytes(StandardCharsets.UTF_8));
                            }
                        },
                        props,
                        AT_LEAST_ONCE // 写出的一致性语义: 目前只能选至少一次
                ));

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
