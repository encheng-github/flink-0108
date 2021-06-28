package com.atguigu.day02.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class Flink02_Source_Collection {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //TODO 从java集合中获取数据
        DataStreamSource<Integer> collectionDStream =
                env.fromCollection(Arrays.asList(1, 2, 3, 4, 5));

        // TODO: 从文件中获取数据
        DataStreamSource<String> textFileDStream = env.readTextFile("input");

        // TODO: 从socket读取数据
        DataStreamSource<String> socketTextDStream = env.socketTextStream("hadoop102", 9999);

        // TODO: 获取元素数据
        DataStreamSource<String> elementDStream = env.fromElements("1", "2",
                "3");

        // TODO: 从kafka获取数据
        // 0.Kafka相关配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.setProperty("group.id", "Flink01_Source_Kafka");
        properties.setProperty("auto.offset.reset", "latest");


        DataStreamSource kafkaDStream = env.addSource(new FlinkKafkaConsumer<>("flink-0108",
                new SimpleStringSchema(), properties));

        collectionDStream.print("collection");
        textFileDStream.print("textFile");
        socketTextDStream.print("socket");
        elementDStream.print("element");
        kafkaDStream.print("kafka");

        env.execute();
    }
}
