package com.tomtop.flink.D_Operator;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author:txf
 * @Date:2022/12/11 9:40
 * 例子：
 * 需求：需要查询数据库才能确定如何对value做什么样的操作。这里就需要连接数据库了。
 * 问题：如果一个元素建立一个连接, 连接数过多, 效率太低。
 * 思考：flink中应该是一个并行度建立一个到数据库的连接，数据库连接建立的时机: 应该所有的数据道来之前建立，接着连接的关闭: 应该是等到所有的数据都处理完了, 或者程序停止之前关闭
 *
 * 解决：因此每个算子都有Rich修饰的算子函数类，称为富有的算子函数类。Rich修饰的这个类中都会多出两个方法:open，和 close 方法：而且每个并行度执行一次。
 *
 */
public class Flink02_Map_Rich {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4, 5, 6, 7);
        source.map(new RichMapFunction<Integer,Integer>() {
            //这个方法在程序启动时调用，这个方法是每个并行度中只执行一次。一般是在这个方法中创建与数据库的连接，开辟资源。
            @Override
            public void open(Configuration parameters) throws Exception {
                // 建立到数据库的连接
                // 查询
                // 得到结果
                // 关闭数据库的连接
                System.out.println(" 测试运行open方法，测试数据库");
            }

            @Override
            public Integer map(Integer value) throws Exception {
                System.out.println("map:"+value);
                return value * value;
            }

            @Override
            public void close() throws Exception {
                System.out.println(" 测试运行close方法");
            }
        })
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
