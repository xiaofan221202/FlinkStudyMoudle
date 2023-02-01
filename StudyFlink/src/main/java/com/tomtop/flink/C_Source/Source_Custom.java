package com.tomtop.flink.C_Source;

import com.tomtop.flink.A_Bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * @Author:txf
 * @Date:2022/12/9 17:55
 */
public class Source_Custom {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.addSource(new MySocketSource());
        waterSensorDataStreamSource.print();

        try {
            env.execute("test custom source");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    public static class MySocketSource implements SourceFunction<WaterSensor>{

        boolean isStop = false;

        //开启source
        @Override
        public void run(SourceContext<WaterSensor> sourceContext) throws Exception {
            Socket socket = new Socket("localhost", 9000);
            InputStream inputStream = socket.getInputStream();
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);//StandardCharsets.UTF_8设置字符流编码集
            BufferedReader inputStreamBufferedReader = new BufferedReader(inputStreamReader);
            String line = inputStreamBufferedReader.readLine();//一行一行读

            while (!isStop && line !=null){
                String[] data = line.split(",");
                // 把数据放入到流中,一行读切分封装javaBean WaterSensor
                sourceContext.collect(new WaterSensor(data[0], Long.valueOf(data[1]), Integer.valueOf(data[2])));
                line = inputStreamBufferedReader.readLine();
            }

        }

        //关闭source ,这个方法不会自动调用, 如果有需要的时候, 外部可以通过调用这个方法实现关闭source
        @Override
        public void cancel() {
            isStop = true;
        }
    }
}


