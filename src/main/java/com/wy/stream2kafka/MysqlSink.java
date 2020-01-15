package com.wy.stream2kafka;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * @author wangyn3
 * @date 2020-01-13 13:32
 */
public class MysqlSink extends RichSinkFunction<String> {

    @Override
    public void invoke(String value, Context context) throws SQLException {
        String url = "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false&autoReconnect=true";
        Connection connection = null;
        try {
            connection = DBConnectUtil.getConnection(url, "root", "123456");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.err.println("start beginTransaction......."+connection);
        System.out.println("=============="+value);
        String sql = "insert into `t_test` (`name`,`insert_time`) values (?,?)";
        PreparedStatement ps = null;


        try {
            ps = connection.prepareStatement(sql);
            ps.setString(1, value);
            ps.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
            //执行insert语句
            ps.execute();
            if("asdasd".equals(value)){
                System.out.println(1/0);
            }
            DBConnectUtil.commit(connection);
        } catch (SQLException e) {
            connection.rollback();
            e.printStackTrace();
        }






    }


}
