package com.tomtop.doris;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.cfg.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @Author:txf
 * @Date:2022/12/12 17:34
 * */


public class DataStreamStringDorisSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("format","json");
        properties.setProperty("strip_outer_array","true");

        DataStreamSink<String> stringDataStreamSink = env
                .fromElements("{\"siteid\": \"66\", \"citycode\": \"6\", \"username\": \"pengyuyan\",\"pv\": \"6\"}")
                .addSink(
                        DorisSink.sink(//这里报错原因是版本依赖问题。doris1.1好像不支持这样的写法。先注销掉
                                DorisReadOptions.builder().build(),
                                DorisExecutionOptions.builder()
                                        .setBatchSize(3)
                                        .setBatchIntervalMs(0L)
                                        .setMaxRetries(3)
                                        .setStreamLoadProp(properties)
                                        .build(),
                                DorisOptions.builder()
                                        .setFenodes("hadoop1:8030")
                                        .setTableIdentifier("test_db.table")
                                        .setUsername("test")
                                        .setPassword("test")
                                        .build()
                        ));
        env.execute();
    }
}
