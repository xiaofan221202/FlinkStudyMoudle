package com.tomtop.flink.C_Source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author:txf
 * @Date:2022/12/9 17:41
 */
public class Source_Kafka_Test2 {
    public static void main(String[] args) throws Exception {
        //设置执行环境
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        KafkaSource<String> source = KafkaSource
                .<String>builder()  //这里泛型写String,是因为我们测试消费者需要的数据类型。这里是语法。不能推导出来，自己写
                .setBootstrapServers("IP:9092,IP:9092,IP:9092") //设置集群地址
                .setGroupId("tomtop")  //设置消费组
                .setTopics("topic") //设置消费主题
                .setStartingOffsets(OffsetsInitializer.latest())  //设置消费主题的偏移量
                .setValueOnlyDeserializer(new SimpleStringSchema())  //这里的反序列化器和builder()泛型相需要对应
                .build();

        //参数解读：source:需要加入的流，WatermarkStrategy.noWatermarks():不用水印，kafka_source：给流取名
        DataStreamSource<String> streamSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka_source");
        streamSource.print();

        env.execute("test consumer kafka topic as source create stream");
    }
}
