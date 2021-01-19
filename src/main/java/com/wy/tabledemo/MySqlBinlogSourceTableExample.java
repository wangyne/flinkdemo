package com.wy.tabledemo;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class MySqlBinlogSourceTableExample {

    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner().inStreamingMode() //1.11后默认
                .build();
//        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment tEnv=StreamTableEnvironment.create(executionEnvironment, settings);

        TableEnvironment tEnv = TableEnvironment.create(settings);
        tEnv.executeSql("CREATE TABLE person(\n" +
                "  id int,\n" +
                "  name STRING,\n" +
                "  age int,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'mysql-cdc',\n" +
                "  'hostname' = 'localhost',\n" +
                "  'port' = '3306',\n" +
                "  'username' = 'root',\n" +
                "  'password' = '123456',\n" +
                "  'database-name' = 'test',\n" +
                "  'table-name' = 'person'\n" +
                ")");
        tEnv.executeSql("CREATE TABLE person_1(\n" +
                "  id int,\n" +
                "  name STRING,\n" +
                "  age int,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                " 'driver'='com.mysql.cj.jdbc.Driver'," +
                "  'username' = 'root',\n" +
                "  'password' = '123456',\n" +
                "  'url' = 'jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC'," +
//                "  'database-name' = 'test',\n" +
                "  'table-name' = 'person_1'\n" +
                ")");

        Table table = tEnv.sqlQuery("select * from person_1");
        table.execute().print();
        TableResult tableResult = tEnv.executeSql("insert into person_1 select * from person");

//        tableResult.print();

    }
}
