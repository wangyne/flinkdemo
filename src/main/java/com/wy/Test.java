package com.wy;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.descriptors.*;
//import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.api.common.typeinfo.Types;
//import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
//import org.apache.flink.table.descriptors.ConnectorDescriptor;
import java.io.File;
import java.io.IOException;

/**
 * @author wangyn3
 * @date 2019-12-27 15:21
 */
public class Test {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env=ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnvironment= BatchTableEnvironment.getTableEnvironment(env);

//        String path = Test.class.getClassLoader().getResource("words.txt").getPath();
//        String path="C:\\workspace\\finkdemo\\src\\main\\resources\\words.txt";
        String path="/usr/local/games/words.txt";
        tableEnvironment.connect(new FileSystem().path(path))
                .withFormat(new Csv().field("word", Types.STRING).lineDelimiter("\n"))
                .withSchema(new Schema().field("word", Types.STRING))
                .registerTableSource("wordcount");
        Table result=tableEnvironment.scan("wordcount")
                .groupBy("word")
                .select("word, count(1) as count");
        tableEnvironment.toDataSet(result,Row.class).print();
    }
}
