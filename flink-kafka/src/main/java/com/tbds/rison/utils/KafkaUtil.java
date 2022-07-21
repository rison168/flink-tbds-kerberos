package com.tbds.rison.utils;

import org.apache.flink.kafka.shaded.org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.impl.BeanAsArrayDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StringDeserializer;

import java.util.Properties;

/**
 * @PACKAGE_NAME: com.tbds.rison.utils.KafkaUtil
 * @NAME: KafkaUtil
 * @USER: Rison
 * @DATE: 2022/2/8 16:37
 * @PROJECT_NAME: flink-iceberg
 **/
public class KafkaUtil {
    /**
     * kafka 消费者 properties
     * @param servers kafka broker servers
     * @param groupId groupId
     * @return
     */
    public static Properties consumerProps(String servers, String groupId) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put("auto.offset.reset", "latest");
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "3000");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", BeanAsArrayDeserializer.class.getName());
        //设置SASL_PLAINT认证
//        props.put("security.protocol", "SASL_PLAINTEXT");
//        props.put("sasl.mechanism", "PLAIN");
//        props.put("sasl.jaas.config", "org.apache.flink.kafka.shaded.org.apache.kafka.common.security.scram.ScramLoginModule required username=\"kafka\" password=\"kafka@Tbds.com\";");

        //设置kerberos认证
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.kerberos.service.name", "kafka");
        props.put("sasl.mechanism", "GSSAPI");

        return props;
    }

    /**
     * kafka 生产者 properties
     * @param servers kafka broker servers
     * @param groupId groupId
     * @return
     */
    public static Properties producerProps(String servers, String groupId){
        Properties props = new Properties();
        props.put("bootstrap.servers", servers);
        props.put("group.id", groupId);
        props.put("acks", "all");
        /**
         * 设置大于零的值时，Producer会发送失败后会进行重试。
         */
        props.put("retries", 1);
        /**
         * 5 mins
         */
        props.put("transaction.max.timeout.ms", Integer.toString(300_000));
        /**
         * Producer批量发送同一个partition消息以减少请求的数量从而提升客户端和服务端的性能，默认大小是16348 byte(16k).
         * 发送到broker的请求可以包含多个batch, 每个batch的数据属于同一个partition，太小的batch会降低吞吐.太大会浪费内存.
         */
        props.put("batch.size", 16384);
        /**
         * batch.size和liner.ms配合使用，前者限制大小后者限制时间。前者条件满足的时候，同一partition的消息会立即发送,
         * 此时linger.ms的设置无效，假如要发送的消息比较少, 则会等待指定的时间以获取更多的消息，此时linger.ms生效 默认设置为0ms(没有延迟).
         */
        props.put("linger.ms", 1);
        /**
         * Producer可以使用的最大内存来缓存等待发送到server端的消息.默认值33554432 byte(32m)
         */
        props.put("buffer.memory", 33554432);
        props.put("max.request.size", 10485760);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        //设置SASL_PLAINT认证
//        props.put("security.protocol", "SASL_PLAINTEXT");
//        props.put("sasl.mechanism", "PLAIN");
//        props.put("sasl.jaas.config", "org.apache.flink.kafka.shaded.org.apache.kafka.common.security.scram.ScramLoginModule required username=\"kafka\" password=\"kafka@Tbds.com\";");

        //设置kerberos认证
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.kerberos.service.name", "kafka");
        props.put("sasl.mechanism", "GSSAPI");
        return props;
    }
}
