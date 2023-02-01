package com.tomtop.flink.C_Source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.Arrays;
import java.util.List;

/**
 * @Author:txf
 * @Date:2022/12/9 16:28
 */
public class Source_Read_ListAndSocket {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        //env.setParallelism(1) //不设置最高级是配置文件中，和电脑CUP线程一样
        //env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);//不设置运行模式默认是？？

        DataStreamSource<String> readStream = env.readTextFile("src/input/word.txt");

        DataStreamSource<String> readStream2 = env.fromElements("{\"siteid\": \"66\", \"citycode\": \"6\", \"username\": \"pengyuyan\",\"pv\": \"6\"}");

        List<String> list = Arrays.asList("a", "aa", "b", "c", "d");
        DataStreamSource<String> readStream3 = env.fromCollection(list);

        DataStreamSource<String> socketStream = env.socketTextStream("localhost", 9000);

        readStream.print();
        readStream2.print();
        readStream3.print();
        socketStream.print();

        try {
            env.execute("test source stream");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
