package com.wy.stream2kafka.util;

import com.wy.stream2kafka.pojo.User;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;



/**
 * @author wangyn3
 * @date 2020-01-08 19:08
 */
public class AsyncDatabaseRequest extends RichAsyncFunction<User, User> {

    private static String jdbcUrl = "jdbc:mysql://localhost:3306/test?useSSL=false";
    private static String username = "root";
    private static String password = "123456";
    private static String driverName = "com.mysql.jdbc.Driver";
    Connection conn;
    PreparedStatement ps;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName(driverName);
        conn = DriverManager.getConnection(jdbcUrl, username, password);
        ps = conn.prepareStatement("select phone from test.async_test where id = ?");
    }

    @Override
    public void close() throws Exception {
        super.close();
        conn.close();

    }

    @Override
    public void asyncInvoke(User user, ResultFuture<User> resultFuture) throws Exception {
//        System.out.println(user.getId());
        String id=user.getId();
        ps.setString(1, id);
        ResultSet rs = ps.executeQuery();
        List<User> list = new ArrayList<User>();
        String phone = null;
        while (rs.next()) {
            System.out.println(rs.getString("phone"));
            phone = rs.getString("phone");
            list.add(new User(id,phone));
        }
//        System.out.println(list.size());
        // 放回 result 队列
        resultFuture.complete(list);
    }

    @Override
    public void timeout(User user, ResultFuture<User> resultFuture) {
        List<User> list = new ArrayList<User>();
        list.add(user);
        resultFuture.complete(list);
    }


}
