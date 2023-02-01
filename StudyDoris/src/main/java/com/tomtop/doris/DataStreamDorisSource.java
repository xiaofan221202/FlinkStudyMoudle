package com.tomtop.doris;

import org.apache.doris.flink.cfg.DorisStreamOptions;
import org.apache.doris.flink.datastream.DorisSourceFunction;
import org.apache.doris.flink.deserialization.SimpleListDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Properties;

/**
 * @Author:txf
 * @Date:2022/12/12 17:19
 */
public class DataStreamDorisSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.put("fenodes","ip:8030");
        properties.put("username","test");
        properties.put("password","test");
        properties.put("table.identifier","test_db.table");

        //注意DorisSource的反序列化类型是SimpleListDeserializationSchema;任意类型。
        DataStreamSource<List<?>> listDataStreamSource = env
                .addSource(new DorisSourceFunction(new DorisStreamOptions(properties), new SimpleListDeserializationSchema()));
        listDataStreamSource.print();

        env.execute();

    }
}
