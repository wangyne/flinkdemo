package com.wy.stream2kafka;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * @author wangyn3
 * @date 2019-12-30 11:25
 */
public class MySqlTwoPhaseCommitSink extends TwoPhaseCommitSinkFunction<String, Connection, Void> {


    public MySqlTwoPhaseCommitSink() {
        super(new KryoSerializer<>(Connection.class, new ExecutionConfig()), VoidSerializer.INSTANCE);
    }

    /**
     * 执行数据库写入操作
     */
    @Override
    protected void invoke(Connection connection, String value, Context context)  {
//        String value = objectNode.get("value").toString();
        System.out.println("=============="+value);
        String sql = "insert into `t_test` (`name`,`insert_time`) values (?,?)";
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(sql);
            ps.setString(1, value);
            ps.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
            //执行insert语句
            ps.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

//        if("asdasd".equals(value)){
//            System.out.println(1/0);
//        }
    }

    /**
    * 获取连接，开启手动提交事务
    * */

    @Override
    protected Connection beginTransaction() throws Exception {
        String url = "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false&autoReconnect=true";
        Connection connection = DBConnectUtil.getConnection(url, "root", "123456");
        System.err.println("start beginTransaction......."+connection);
        return connection;
    }

    /**
     * 预提交，提交逻辑在invoke方法中
     * */
    @Override
    protected void preCommit(Connection connection) throws Exception {
        System.err.println("start preCommit......."+connection);
    }

    /**
     * 如果invoke执行正常则提交事务
     * */
    @Override
    protected void commit(Connection connection) {
        System.err.println("start commit......."+connection);
        DBConnectUtil.commit(connection);
    }

    /**
     * 如果invoke执行异常则回滚事物，下一次的checkpoint操作也不会执行
     * */
    @Override
    protected void abort(Connection connection) {
        System.err.println("start abort rollback......."+connection);
        DBConnectUtil.rollback(connection);
    }


}
