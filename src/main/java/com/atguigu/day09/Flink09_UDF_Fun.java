package com.atguigu.day09;

import com.atguigu.day02.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class Flink09_UDF_Fun {
    public static void main(String[] args) {
        //1.流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> dataStreamSource = env.socketTextStream("hadoop102", 9999).map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String s) throws Exception {
                String[] split = s.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //2.表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //3.将流转为表
        Table table = tableEnv.fromDataStream(dataStreamSource);

        // TODO: 4.不注册函数直接使用
//        table.select($("id"),call(MyUDF.class,$("id"))).execute().print();

        // TODO: 先注册再使用
        tableEnv.createTemporaryFunction("idLen",MyUDF.class);

        //API
//        table.select($("id"),call("idLen",$("id")).as("idLength")).execute().print();

        //SQL
        tableEnv.executeSql("select id,idLen(id) from "+table).print();


    }
    // TODO: 自定义udf函数(返回id字符串长度)
    public static class MyUDF extends ScalarFunction{

        public int eval(String value){
            return value.length();
        }

    }
}
