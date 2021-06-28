package com.atguigu.day08;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class Flink04_SQL_TableAPI_File_Source_2 {
    public static void main(String[] args) throws Exception {
        //1.获取流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // TODO: 3.获取文件系统的数据,转为动态表

        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());

        tableEnv.connect(new FileSystem().path("input/sensor-sql.txt"))
                //指定读取格式
                .withFormat(new Csv().fieldDelimiter(',').lineDelimiter("\n"))
                //动态表都有哪些字段,分别是什么类型
                .withSchema(schema)
                //将读过来的数据写入到临时表中
                .createTemporaryTable("sensor");

        // TODO: 将表转为对象
        Table sensorTable = tableEnv.from("sensor");

//        Table result = sensorTable
//                .where($("id").isEqual("sensor_1"))
//                .select($("id"), $("ts"), $("vc"));
//        //将动态表转为流
//        tableEnv.toAppendStream(result, Row.class).print();

        //聚合操作
        Table result = sensorTable
                .groupBy($("id"))
                .select($("id"), $("vc").sum().as("vcSum"));

        tableEnv.toRetractStream(result,Row.class).print();

        env.execute();
    }
}
