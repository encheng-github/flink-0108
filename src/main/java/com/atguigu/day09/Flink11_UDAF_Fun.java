package com.atguigu.day09;

import com.atguigu.day02.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class Flink11_UDAF_Fun {
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
                .groupBy($("id"))
                .select($("id"),call(MyUDAF.class,$("vc")))
                .execute()
                .print();
*/
        // TODO: 先注册再使用
        tableEnv.createTemporaryFunction("avgFun",MyUDAF.class);

        //API
    /*    table
                .groupBy($("id"))
                .select($("id"),call("avgFun",$("vc")).as("vcAvg"))
                .execute()
                .print();*/

        //SQL
        tableEnv.executeSql("select id,avgFun(vc) from "+table+" group by id").print();


    }
    // TODO: 自定义udaf函数(vc的平均值)
    public static class MyUDAF extends AggregateFunction<Double,
            Tuple2<Integer,Integer>>{

        //返回结果值
        @Override
        public Double getValue(Tuple2<Integer, Integer> integerIntegerTuple2) {
            return integerIntegerTuple2.f0*1D/integerIntegerTuple2.f1;
        }

        //累加操作
        public void accumulate(Tuple2<Integer, Integer> acc,Integer value){
            acc.f0+=value;
            acc.f1+=1;
        }

        //创建累加器
        @Override
        public Tuple2<Integer, Integer> createAccumulator() {
            return Tuple2.of(0,0);
        }
    }

}
