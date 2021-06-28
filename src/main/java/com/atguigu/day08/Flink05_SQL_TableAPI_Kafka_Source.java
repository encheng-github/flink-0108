package com.atguigu.day08;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class Flink05_SQL_TableAPI_Kafka_Source {
    public static void main(String[] args) throws Exception {
        //1.获取流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // TODO: 3.读取kafka的数据,转为动态表

        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());

        tableEnv.connect(new Kafka()
                .version("universal")
                .topic("sensor")
                .property("group.id", "bigdata")
                .startFromLatest()
                .property("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
        )
                .withFormat(new Csv().fieldDelimiter(','))
                .withSchema(schema)
                .createTemporaryTable("sensor_kafka");


        // TODO: 将临时表转为对象
        Table sensorKafka = tableEnv.from("sensor_kafka");

        Table result = sensorKafka
                .where($("id").isEqual("sensor_1"))
                .select($("id"), $("ts"), $("vc"));

        //将动态表转为流
        tableEnv.toAppendStream(result, Row.class).print();

        env.execute();
    }
}
