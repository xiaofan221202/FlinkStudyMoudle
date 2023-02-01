import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author:txf
 * @Date:2022/12/13 9:56
 */
public class FlinkSQLDatagenToDoris {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000);
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env,environmentSettings);

        //获取Datagan数据作为source
        tenv.executeSql("CREATE TABLE order_info_source (\n" +
                "    order_id     INT primary key,\n" +
                "    order_date DATE,\n" +
                "    buy_num      INT,\n" +
                "    user_id      INT,\n" +
                "    create_time  TIMESTAMP(3),\n" +
                "    update_time   TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second' =  '10',\n" +
                "  'fields.order_id.min' = '30001',\n" +
                "  'fields.order_id.max' = '30500',\n" +
                "  'fields.user_id.min' = '10001',\n" +
                "  'fields.user_id.max' = '20001',\n" +
                "  'fields.buy_num.min' = '10',\n" +
                "  'fields.buy_num.max' = '20',\n" +
                "  'number-of-rows' = '100'" +
                ")");


/*            // 测试用于查看datagen生成的数据
            tenv.executeSql("CREATE TABLE print_table (\n" +
                    "    order_date DATE,\n" +
                    "    order_id     INT,\n" +
                    "    buy_num      INT,\n" +
                    "    user_id      INT,\n" +
                    "    create_time  TIMESTAMP(3),\n" +
                    "    update_time   TIMESTAMP(3)\n" +
                    ") WITH (\n" +
                    "  'connector' = 'print'\n" +
                    ")");
            tenv.executeSql("insert into print_table select * from order_info_source");
            tenv.executeSql("select * from print_table");*/


        //注册Doris Sink表
        tenv.executeSql("CREATE TABLE order_info_sink (  \n" +
                "order_id INT primary key,  \n" +
                "order_date DATE,  \n" +
                "buy_num INT,\n" +
                "user_id INT,\n" +
                "create_time TIMESTAMP(3),\n" +
                "update_time TIMESTAMP(3)\n" +
                ")  \n" +
                "WITH (\n" +
                "'connector' = 'doris',   \n" +
                "'fenodes' = '172.18.1.18:8030',   \n" +
                "'table.identifier' = 'qulv2.order_info_sink',   \n" +
                "'username' = 'tomtop',   \n" +
                "'password' = 'qulv123',   \n" +
                "'sink.label-prefix' = 'doris_label'\n" +
                ")"
        );


        tenv.executeSql("insert into order_info_sink select * from order_info_source");

    }
}
