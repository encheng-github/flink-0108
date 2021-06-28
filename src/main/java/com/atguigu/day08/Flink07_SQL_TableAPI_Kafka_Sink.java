package com.atguigu.day08;

import com.atguigu.day02.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class Flink07_SQL_TableAPI_Kafka_Sink {
    public static void main(String[] args) throws Exception {
        //1.获取流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //准备数据
        DataStreamSource<WaterSensor> waterSensorStream =
                env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60));


        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.1 将流转为动态表
        Table table = tableEnv.fromDataStream(waterSensorStream);

        //2.2 调用TableAPI执行查询
        Table resultTable = table
                .where($("id").isEqual("sensor_1"))
                .select($("id"), $("ts"), $("vc"));

        // TODO: 3.创建输出到kafka的动态表
        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());

        tableEnv.connect(new Kafka()
                .version("universal")
                .topic("sensor")
                .sinkPartitionerRoundRobin()
                .property("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
        )
                .withFormat(new Json())
                .withSchema(schema)
                .createTemporaryTable("sensor_kafka");


        // TODO: 将查询好的数据写入到刚建好的临时表中
        resultTable.executeInsert("sensor_kafka");

    }
}
