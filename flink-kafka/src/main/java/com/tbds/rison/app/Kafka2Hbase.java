package com.tbds.rison.app;

import com.alibaba.fastjson.JSON;
import com.tbds.rison.bean.User;
import com.tbds.rison.func.SinkToHbaseFunction;
import com.tbds.rison.trigger.UserTrigger;
import com.tbds.rison.utils.GsonUtil;
import com.tbds.rison.utils.KafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * @PACKAGE_NAME: com.tbds.rison.app
 * @NAME: Kafka2Hbase
 * @USER: Rison
 * @DATE: 2022/7/13 10:32
 * @PROJECT_NAME: flink-tbds-kerberos
 **/
public class Kafka2Hbase {
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
                .windowAll(TumblingProcessingTimeWindows.of(Time.minutes(2)))
                .trigger(UserTrigger.of(10L))
                .apply(new AllWindowFunction<User, List<User>, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<User> values, Collector<List<User>> out) throws Exception {
                        ArrayList<User> user = Lists.newArrayList(values);
                        if (user.size() > 0) {
                            System.out.println("collect data rows ：" + user.size());
                            out.collect(user);
                        }
                    }
                })
                .addSink(new SinkToHbaseFunction());

        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3,
                org.apache.flink.api.common.time.Time.of(2, TimeUnit.SECONDS),
                org.apache.flink.api.common.time.Time.of(5, TimeUnit.SECONDS)
        ));
        env.execute("kafka2hbase");
    }
}
/*
kafka info:
{"uid":"1001","name":"rison","code":"1"}
{"uid":"1002","name":"wangwu","code":"2"}
{"uid":"1003","name":"zhangsan","code":"3"}
{"uid":"1004","name":"lisi","code":"4"}
{"uid":"1005","name":"jay","code":"5"}
{"uid":"1006","name":"bob","code":"6"}
{"uid":"1007","name":"jack","code":"7"}
{"uid":"1008","name":"json","code":"8"}
{"uid":"1009","name":"wangfang","code":"9"}
{"uid":"1010","name":"zhaoliu","code":"10"}
{"uid":"1011","name":"liuyi","code":"11"}
{"uid":"1012","name":"cood","code":"12"}
{"uid":"1013","name":"monitor","code":"13"}
{"uid":"1014","name":"yihan","code":"14"}
{"uid":"1015","name":"tianl","code":"15"}
{"uid":"1016","name":"ters","code":"16"}
--
 */