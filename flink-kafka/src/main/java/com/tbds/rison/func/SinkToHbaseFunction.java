package com.tbds.rison.func;

import com.tbds.rison.bean.User;
import com.tbds.rison.utils.RowKeyUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.hbase.shaded.org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.flink.hbase.shaded.org.apache.hadoop.hbase.TableName;
import org.apache.flink.hbase.shaded.org.apache.hadoop.hbase.client.*;
import org.apache.flink.hbase.shaded.org.apache.hadoop.hbase.util.Bytes;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @PACKAGE_NAME: com.tbds.rison.func
 * @NAME: SinkToHbaseFunction
 * @USER: Rison
 * @DATE: 2022/7/13 14:24
 * @PROJECT_NAME: flink-tbds-kerberos
 **/
public class SinkToHbaseFunction extends RichSinkFunction<List<User>> {
    private final static Logger logger = LoggerFactory.getLogger(SinkToHbaseFunction.class);
    private static String TABLE_NAME = "user_info";
    private static String AUTH = "kerberos";
    private static String KEYTAB_FILE = "/etc/security/keytabs/hbase.headless.keytab";
    private static String PRINCIPAL = "hbase-tdw@TBDS.COM";
    private static String KRB5_CONF = "/etc/krb5.conf";
    private static String ZOOKEEPER_SERVERS = "tbds-172-16-16-142:2181,tbds-172-16-16-87:2181,tbds-172-16-16-91:2181";
    private static String ZOOKEEPER_PORT = "2181";

    Connection connection = null;
    private BufferedMutator bufferedMutator = null;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        System.setProperty("java.security.krb5.conf", KRB5_CONF);
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", ZOOKEEPER_SERVERS);
        conf.set("zookeeper.znode.parent", "/hbase-secure");
        conf.set("hadoop.security.authentication", AUTH);
        conf.set("hbase.client.keytab.file", KEYTAB_FILE);
        conf.set("hbase.client.keytab.principal", PRINCIPAL);
        conf.addResource(new FileInputStream("/etc/hbase/conf/hbase-site.xml"));
        conf.addResource(new FileInputStream("/etc/hbase/conf/core-site.xml"));

        connection = ConnectionFactory.createConnection(
                conf,
                new ThreadPoolExecutor(
                        20, 100, 60,
                        TimeUnit.MINUTES,
                        new LinkedBlockingDeque<>(20)
                )
        );
        logger.info("conn : {}", connection.isClosed());
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(TABLE_NAME));
        params.writeBufferSize(10 * 1024 * 1024L);
        params.setWriteBufferPeriodicFlushTimeoutMs(5 * 1000L);
        try {
            bufferedMutator = connection.getBufferedMutator(params);
        } catch (IOException e) {
            logger.error("get bufferedMutator fail：{}", e.getMessage());
        }

    }

    @Override
    public void invoke(List<User> value, Context context) throws Exception {
        ArrayList<Put> puts = new ArrayList<>();
        value.forEach(
                user -> {
                    try {
                        puts.add(setDataSourcePut(user));
                    } catch (ParseException e) {
                        e.printStackTrace();
                        logger.error("put hbase data fail: {}", e.getMessage());
                    }
                }
        );
        bufferedMutator.mutate(puts);
        bufferedMutator.flush();
    }

    @Override
    public void close() throws Exception {
        if (bufferedMutator != null) {
            bufferedMutator.close();
        }
        if (!connection.isClosed()) {
            connection.close();
        }
    }

    private Put setDataSourcePut(User user) throws ParseException {
        byte[] rowKey = RowKeyUtil.getRowKey(user);
        String cf = "info";
        Put put = new Put(rowKey);
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("uid"), Bytes.toBytes(user.getUid()));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("code"), Bytes.toBytes(user.getCode()));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("Name"), Bytes.toBytes(user.getName()));
        return put;
    }
}
