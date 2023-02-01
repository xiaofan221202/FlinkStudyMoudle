package com.tomtop.flink.C_Source;

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.deserialization.SimpleListDeserializationSchema;
import org.apache.doris.flink.source.DorisSource;
import org.apache.doris.flink.source.DorisSourceBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

/**
 * @Author:txf
 * @Date:2022/12/9 17:55
 */
public class Source_Doris {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        DorisOptions build = DorisOptions.builder()
                .setFenodes("172.18.1.18:8030")  //FE节点IP和端口
                .setTableIdentifier("qulv2.erp_order_id")
                .setUsername("tomtop")
                .setPassword("qulv123")
                .build();

        DorisSource<List<?>> dorisSource = DorisSourceBuilder
                .<List<?>>builder() //这里是泛型指任意泛型。后期根据doris中数据源进行更改
                .setDorisOptions(build)
                .setDorisReadOptions(DorisReadOptions.builder().build()) //
                .setDeserializer(new SimpleListDeserializationSchema()) //这里的反序列化类型和.<List<?>>builder()的类型相对应
                .build();

        env.fromSource(dorisSource, WatermarkStrategy.noWatermarks(), "doris source").print();


    }
}
