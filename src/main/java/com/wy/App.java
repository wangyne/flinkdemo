package com.wy;

import com.wy.stream2kafka.DBConnectUtil;

import java.sql.*;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws SQLException {

        System.out.println(new Timestamp(System.currentTimeMillis()) );
        String url = "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false&autoReconnect=true";
        Connection connection = DBConnectUtil.getConnection(url, "root", "123456");

        String sql = "select phone from test.async_test where id = ?";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setString(1, "3");
//        ps.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
        //执行insert语句
        ResultSet rs=ps.executeQuery();
        while (rs.next()){
            System.out.println(rs.getString(1));
        }
        connection.close();
//        ps.execute();
//        connection.setAutoCommit(false);
//        DBConnectUtil.commit(connection);
//    }

//        System.out.println(1/0);
    }
}
