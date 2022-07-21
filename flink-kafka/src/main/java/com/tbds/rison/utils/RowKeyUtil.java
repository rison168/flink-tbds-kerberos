package com.tbds.rison.utils;

import com.tbds.rison.bean.User;
import org.apache.flink.hbase.shaded.org.apache.hadoop.hbase.util.Bytes;
import org.apache.flink.hbase.shaded.org.apache.hadoop.hbase.util.MD5Hash;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @PACKAGE_NAME: com.tbds.rison.utils
 * @NAME: RowKeyUtils
 * @USER: Rison
 * @DATE: 2022/7/13 22:07
 * @PROJECT_NAME: flink-tbds-kerberos
 **/
public class RowKeyUtil {
    public static byte[] getRowKey(User user) throws ParseException {
        StringBuilder stringBuilder = new StringBuilder(10);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:MM:ss");
        stringBuilder.append(sdf.format(new Date()));
        byte[] preKey = Bytes.toBytes(stringBuilder.toString());
        String md5AsHex = MD5Hash.getMD5AsHex(preKey).substring(0, 5);
        return Bytes.toBytes(md5AsHex + "_" + stringBuilder.toString() + "_" + user.getUid());
    }
}
