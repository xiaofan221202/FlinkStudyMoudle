package com.tomtop.flink.C_Source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;


import java.util.Properties;

/**
 * @Author:txf
 * @Date:2022/12/9 17:08
 */
public class Source_Kafka_Test1 {
    public static void main(String[] args) throws Exception {

        //设置执行环境
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        //配置Kafka属性
        Properties props = new Properties();
        props.setProperty("bootstrap.servers","IP:9092,IP:9092,IP:9092");
        props.setProperty("group.id","tomtop");

        //创建kafka消费对象作为source
        /*
        "topic"：消费主题
         new SimpleStringSchema()：序列化
         props：kafka配置属性
        */
        FlinkKafkaConsumer<String>  consumerTopicAsSource = new FlinkKafkaConsumer<>("topic", new SimpleStringSchema(), props);
        //设置开始消费的位置
        consumerTopicAsSource.setStartFromEarliest();

        DataStreamSource<String> sourceStream = env.addSource(consumerTopicAsSource);

        sourceStream.print();
        env.execute("test consumer kafka topic as source create stream");
    }
}
