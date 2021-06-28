package com.atguigu.day09;

import com.atguigu.day02.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class Flink10_UDTF_Fun {
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
      /*  table
                .joinLateral(call(MyUDTF.class,$("id")))
                .select($("id"),$("word"))
                .execute()
                .print();*/

        // TODO: 先注册再使用
        tableEnv.createTemporaryFunction("splits",MyUDTF.class);

        //API
     /*   table
                .joinLateral(call("splits",$("id")))
                .select($("id"),$("word")).execute().print();*/

        //SQL
        tableEnv.executeSql("select id,word from "+table+",lateral " +
                "table(splits(id))").print();


    }
    // TODO: 自定义udf函数(返回id经过切分的数据)
    //hint暗示，主要作用为类型推断时使用
    @FunctionHint(output = @DataTypeHint("ROW<word STRING>"))

    public static class MyUDTF extends TableFunction<Row>{
        public void eval(String value){
            String[] words = value.split("_");
            for (String word : words) {
                collect(Row.of(word));
            }
        }

    }

}
