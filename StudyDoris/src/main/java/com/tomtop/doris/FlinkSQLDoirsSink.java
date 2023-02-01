package com.tomtop.doris;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author:txf
 * @Date:2022/12/13 14:02
 */
public class FlinkSQLDoirsSink {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE flink_doris (\n" +
                "    siteid INT,\n" +
                "    citycode SMALLINT,\n" +
                "    username STRING,\n" +
                "    pv BIGINT\n" +
                "    ) \n" +
                "    WITH (\n" +
                "      'connector' = 'doris',\n" +
                "      'fenodes' = '172.18.1.18:8030',\n" +
                "      'table.identifier' = 'qulv2.flink_doris',\n" +
                "      'username' = 'tomtop',\n" +
                "      'password' = 'qulv123'\n" +
                ")\n");


        // 读取数据
//        tableEnv.executeSql("select * from flink_doris").print();

        // 写入数据
        tableEnv.executeSql("insert into flink_doris(siteid,username,pv) values(222,'wuyanzu',3)");
    }
}
