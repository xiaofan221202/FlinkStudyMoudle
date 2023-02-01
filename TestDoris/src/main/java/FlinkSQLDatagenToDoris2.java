import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author:txf
 * @Date:2022/12/13 9:56
 */
public class FlinkSQLDatagenToDoris2 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000);
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env,environmentSettings);

        //获取Datagan数据作为source
        tenv.executeSql("CREATE TABLE order_info_source (\n" +
                "   print_label_user String PRIMARY KEY,\n" +
                "   print_label_date DATE,\n" +
                "   print_label_time INT,\n" +
                "   print_label_count INT,\n" +
                "   `updated` TIMESTAMP(3),\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second' =  '10',\n" +
                "  'fields.print_label_user.min' = '30001',\n" +
                "  'fields.print_label_user.max' = '30500',\n" +
                "  'fields.print_label_count.min' = '20',\n" +
                "  'fields.print_label_count.max' = '50',\n" +
                "  'number-of-rows' = '50'" +
                ")");


            // 测试用于查看datagen生成的数据
            tenv.executeSql("CREATE TABLE print_table (\n" +
                    "   print_label_user String PRIMARY KEY,\n" +
                    "   print_label_date DATE,\n" +
                    "   print_label_time INT,\n" +
                    "   print_label_count INT,\n" +
                    "   `updated` TIMESTAMP(3),\n" +
                    ") WITH (\n" +
                    "  'connector' = 'print'\n" +
                    ")");
            tenv.executeSql("insert into print_table select * from order_info_source");
            tenv.executeSql("select * from print_table");


/*        //注册Doris Sink表
        tenv.executeSql("CREATE TABLE print_label_user_statistics (  \n" +
                "   print_label_user String PRIMARY KEY,\n" +
                "   print_label_date DATE,\n" +
                "   print_label_time INT,\n" +
                "   print_label_count INT,\n" +
                "   `updated` TIMESTAMP(3),\n" +
                ")  \n" +
                "WITH (\n" +
                "'connector' = 'doris',   \n" +
                "'fenodes' = '192.168.0.127:8030',   \n" +
                "'table.identifier' = 'ttrtdw.print_label_user_statistics',   \n" +
                "'username' = 'ttrtdw',   \n" +
                "'password' = 'ttrtdw@#123Qw',   \n" +
                "'sink.label-prefix' = 'doris_label'\n" +
                ")"
        );


        tenv.executeSql("insert into print_label_user_statistics select * from order_info_source");*/

    }
}
