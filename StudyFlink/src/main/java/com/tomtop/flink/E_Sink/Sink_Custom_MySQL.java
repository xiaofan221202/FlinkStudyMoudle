package com.tomtop.flink.E_Sink;


import com.tomtop.flink.A_Bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @Author:txf
 * @Date:2022/12/11 19:41
 */
public class Sink_Custom_MySQL {
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
                        return new WaterSensor(
                                data[0],
                                Long.valueOf(data[1]),
                                Integer.valueOf(data[2])
                        );
                    }
                })
                .keyBy(new KeySelector<WaterSensor, String>() {
                    @Override
                    public String getKey(WaterSensor value) throws Exception {
                        return value.getId(); //getId()是从WaterSensor这个javaBean类中调用的getId的方法：意思就是把Id当成key,这里可以写成lamdba表达式模式也可以写成方法引用。
                    }
                })
                .sum("vc")
                .addSink(new MySqlSink());

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    //自定义个内部类MySqlSink(连接MySql的sink)继承RichSinkFunction重写open(开启)，close(关闭)，invoke(引用)方法 : 泛型为WaterSensor

    public static class MySqlSink extends RichSinkFunction<WaterSensor>{
        private Connection connection;
        //连接mysql的步骤： 1. 加载驱动  2. 获取连接 3、插入数据(写sql,赋值，执行) 4、关闭资源

        @Override
        public void open(Configuration parameters) throws Exception {
            //1、加载驱动
            Class.forName("com.mysql.jdbc.Driver");
            //2、获取连接
            connection = DriverManager.getConnection("jdbc:mysql://hadoop162:3306/test?useSSL=false", "root", "aaaaaa");
        }

        @Override
        public void close() throws Exception {
            if (connection != null) {
                connection.close();
            }
        }

        @Override
        public void invoke(WaterSensor value, Context context) throws Exception {
            //3、插入数据
            // 3.1、sql语句
            //String sql = "insert into sensor(id,ts,vc)value(?,?,?)"; //主键重复就报错
            //String sql1 = "insert into sensor(id,ts,vc)value(?,?,?) on duplicate key update vc=?" ;// 如果主键不重复就新增, 重复就更新
            String sql2 = "replace into sensor(id, ts, vc)values(?,?,?)"; // 如果主键不重复就新增, 重复就更新
            PreparedStatement ps = connection.prepareStatement(sql2);
            //3.2、给sql中？站位符赋值 (注意mysql中的下表是从1开始)
            ps.setString(1,value.getId());
            ps.setLong(2,value.getTs());
            ps.setInt(3,value.getVc());
            //ps.setInt(4,value.getVc()); //如果选用sql1的写法。那么这里要加上第4个站位符的值。mysql中的主键冲突。值覆盖的话。需要后面加一列放新值。
            //3.3、执行
            ps.execute();
            //connection.commit();//提交执行。这行代码多余 .因为mysql中是自动提交设置的参数为true,如果设置为false，则需要这行代码
            //4、关闭资源
            ps.close();
        }
    }
}
