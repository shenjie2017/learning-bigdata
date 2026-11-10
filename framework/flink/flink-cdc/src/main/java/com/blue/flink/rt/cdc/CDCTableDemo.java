package com.blue.flink.rt.cdc;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class CDCTableDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8082);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);
        env.enableCheckpointing(5000L);

        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);

        tabEnv.executeSql("CREATE TABLE event_source (\n" +
                "                uid bigint PRIMARY KEY NOT ENFORCED,\n" +
                "                id varchar(255),\n" +
                "                url varchar(255),\n" +
                "                ts bigint,\n" +
                "                dt varchar(255)\n" +
                "        ) WITH (\n" +
                "                'connector' = 'mysql-cdc',\n" +
                "                'hostname' = '127.0.0.1',\n" +
                "                'port' = '3306',\n" +
                "                'username' = 'root',\n" +
                "                'password' = '123456',\n" +
                "                'database-name' = 'bigdata',\n" +
                "                'table-name' = 'event'\n" +
                "       )");

        tabEnv.executeSql(
                "CREATE TABLE event_sink (" +
                        "  uid bigint PRIMARY KEY NOT ENFORCED," + // 声明主键，JDBC Sink 会自动启用 Upsert 模式
                        "  id STRING," +
                        "  url STRING," +
                        "  ts BIGINT," +
                        "  dt STRING" +
                        ") WITH (" +
                        "  'connector' = 'upsert-kafka'," +
                        "  'properties.bootstrap.servers' = '127.0.0.1:9092'," +
                        "  'topic' = 'event_record'," +
                        "  'key.format' = 'json'," +
                        "  'value.format' = 'json'" +
                        ")"
        );

        tabEnv.executeSql("INSERT INTO event_sink SELECT uid,id, url, ts, dt FROM event_source");
    }
}
