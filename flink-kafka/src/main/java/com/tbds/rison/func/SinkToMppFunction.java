package com.tbds.rison.func;

import com.tbds.rison.bean.User;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

/**
 * @PACKAGE_NAME: com.tbds.rison.func
 * @NAME: SinkToMppFunction
 * @USER: Rison
 * @DATE: 2022/7/12 10:19
 * @PROJECT_NAME: flink-tbds-kerberos
 **/
public class SinkToMppFunction extends RichSinkFunction<List<User>> {
    PreparedStatement ps;
    BasicDataSource dataSource;
    private Connection connection;
    private static String URL = "jdbc:seaboxsql://172.16.16.7/demodb";
    private static String USER = "seabox";
    private static String PASSWORD = "seabox@123";
    private static String  DRIVER = "com.seaboxsql.Driver";

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        dataSource = new BasicDataSource();
        connection = getConnection(dataSource);
        String sql = "insert into user_tbl(uid, name, code) values(?, ?, ?);";
        ps = this.connection.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
        //close conn and release resources
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    @Override
    public void invoke(List<User> value, Context context) throws Exception {
        for (User user : value) {
            ps.setString(1, user.getUid());
            ps.setString(2, user.getName());
            ps.setString(3,user.getCode());
            ps.addBatch();
        }
        int[] count = ps.executeBatch();
        System.out.println("successfully inserted " + count.length + " row data!");
    }

    private static Connection getConnection(BasicDataSource dataSource) {
        dataSource.setDriverClassName(DRIVER);
        dataSource.setUrl(URL);
        dataSource.setUsername(USER);
        dataSource.setPassword(PASSWORD);
        dataSource.setInitialSize(10);
        dataSource.setMaxTotal(50);
        dataSource.setMinIdle(2);

        Connection con = null;
        try {
            con = dataSource.getConnection();
            System.out.println("create pools：" + con);
        } catch (Exception e) {
            System.out.println("mpp get connection has exception , msg = " + e.getMessage());
        }
        return con;
    }


}
