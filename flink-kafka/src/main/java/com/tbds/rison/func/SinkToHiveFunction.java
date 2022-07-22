package com.tbds.rison.func;

import com.tbds.rison.bean.User;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;


/**
 * @PACKAGE_NAME: com.tbds.rison.func
 * @NAME: SinkToHiveFunction
 * @USER: Rison
 * @DATE: 2022/7/21 9:41
 * @PROJECT_NAME: flink-tbds-kerberos
 **/
public class SinkToHiveFunction extends RichSinkFunction<User> {
    PreparedStatement ps;
    private Connection connection;
    private static String URL = "jdbc:hive2://tbds-172-16-16-87:2181,tbds-172-16-16-142:2181,tbds-172-16-16-91:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2_default";
    private static String USER = "hive";
    private static String PASSWORD = "hive@Tbds.com";
    private static String DRIVER = "org.apache.hive.jdbc.HiveDriver";
    private static String PRINCIPAL = "hive/tbds.instance@TBDS.COM";
    private static String KEYTAB = "/etc/security/keytabs/kdc/hive.tbds.instance.keytab";

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        AuthKrb5(PRINCIPAL, KEYTAB);
        Class.forName(DRIVER);
        connection = DriverManager.getConnection(URL, USER, PASSWORD);
        System.out.println("hive jdbc connection : " + connection);
        String sql = "insert into hive_db.user_tbl(uid, name, code) values(?, ?, ?)";
        ps = this.connection.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
        //close conn and release resources
        if (connection != null) {
            System.out.println("== close conn and release resources ==");
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    @Override
    public void invoke(User user, Context context) throws Exception {
        ps.setString(1, user.getUid());
        ps.setString(2, user.getName());
        ps.setString(3, user.getCode());
        ps.execute();
        System.out.println("successfully insert row data!");
    }


    public static void AuthKrb5(String principal, String keytab) {
        System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.setBoolean("hadoop.security.authorization", true);
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hive.security.authentication", "kerberos");
        conf.set("fs.defaultFS", "hdfs://hdfsCluster");
        conf.set("hadoop.home.dir", "/");
        UserGroupInformation.setConfiguration(conf);
        try {
            UserGroupInformation.loginUserFromKeytab(principal, keytab);
            System.out.println("Kerberos auth success");
        } catch (IOException e) {
            System.out.println("Kerberos auth fail：" + e.getMessage());
        }
    }


}
