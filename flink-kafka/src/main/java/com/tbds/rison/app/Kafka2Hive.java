package com.tbds.rison.app;

import com.alibaba.fastjson.JSON;
import com.tbds.rison.bean.User;
import com.tbds.rison.func.SinkToHiveFunction;
import com.tbds.rison.utils.GsonUtil;
import com.tbds.rison.utils.KafkaUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;
import java.util.regex.Pattern;

/**
 * @PACKAGE_NAME: com.tbds.rison.app
 * @NAME: Kafka2Hive
 * @USER: Rison
 * @DATE: 2022/7/21 9:38
 * @PROJECT_NAME: flink-tbds-kerberos
 **/
public class Kafka2Hive {
    private static String TOPIC = "test_rison";
    private static String KAFKA_SERVERS = "tbds-172-16-16-142:6669,tbds-172-16-16-87:6669,tbds-172-16-16-91:6669";
    private static String GROUP_ID = "test_group_id" + System.currentTimeMillis();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointInterval(10 * 1000L);
        checkpointConfig.setMinPauseBetweenCheckpoints(10 * 1000L);
        checkpointConfig.setTolerableCheckpointFailureNumber(3);
        checkpointConfig.setCheckpointTimeout(60 * 1000L);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>(
                Pattern.compile(TOPIC),
                new SimpleStringSchema(),
                KafkaUtil.consumerProps(KAFKA_SERVERS, GROUP_ID));

        DataStream<String> source = env.addSource(myConsumer);
        source.print("kafka-info:");

        source.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String s, Context context, Collector<String> collector) throws Exception {
                try {
                    if (StringUtils.isNullOrWhitespaceOnly(s)) {
                        System.out.println("JSON IS EMPTY!");
                        return;
                    }
                    Object parse = JSON.parse(s);
                    collector.collect(s);
                } catch (Exception e) {
                    System.out.println("JSON DATA PARSE ERROR ! => " + s + "\n" + e.getMessage());
                }
            }
        })
                .map(data -> GsonUtil.fromJson(data, User.class))
                .addSink(new SinkToHiveFunction());


        env.execute("kafka2Hive");
    }
}
/*
export KAFKA_OPTS='-Djava.security.auth.login.config=/etc/kafka/conf/kafka-client-jaas.conf';
bin/kafka-console-producer.sh --bootstrap-server tbds-172-16-16-142:6669,tbds-172-16-16-87:6669,tbds-172-16-16-91:6669 --topic test_rison --producer.config /tmp/kafka-client-jaas.properties
 */
