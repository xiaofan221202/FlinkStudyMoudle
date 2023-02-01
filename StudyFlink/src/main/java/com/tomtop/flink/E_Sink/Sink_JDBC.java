package com.tomtop.flink.E_Sink;


import com.tomtop.flink.A_Bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Author:txf
 * @Date:2022/12/11 18:59
 * 实现步骤：调用JdbcSink类的sink方法实现，难点是sink方法中4个参数的配置
 */
public class Sink_JDBC {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env
                .socketTextStream("localhost",9000)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] data = value.split(",");
                        return  new WaterSensor(
                                data[0],
                                Long.valueOf(data[1]),
                                Integer.valueOf(data[2])
                        );
                    }
                })
                .keyBy(WaterSensor::getId)
                .sum("vc")
                .addSink(JdbcSink.sink(
                        //参数一：sql语句
                        "replace into sensor(id,ts,vc)value(?,?,?)",
                        //参数二：给sql中的站位符进行赋值，千万不要关闭ps,ps要重用
                        new JdbcStatementBuilder<WaterSensor>() {
                            @Override
                            public void accept(PreparedStatement ps, WaterSensor ws) throws SQLException {
                                ps.setString(1,ws.getId());
                                ps.setLong(2,ws.getTs());
                                ps.setInt(3,ws.getVc());
                            }
                        },
                        //参数三：JDBC的执行选择构建和配置
                        new JdbcExecutionOptions.Builder()
                                .withBatchIntervalMs(1000)
                                .withBatchSize(1024)
                                .withMaxRetries(3)
                                .build(),
                        //参数四：JDBC连接connection的构建和配置
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withDriverName("com.mysql.cj.jdbc.Driver")
                                .withUrl("jdbc:mysql://hadoop162:3306/test?useSSL=false")
                                .withUsername("root")
                                .withPassword("aaaaaa")
                                .build()
                ));

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
