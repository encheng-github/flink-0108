package com.atguigu.day09;

import com.atguigu.day02.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class Flink12_UDATF_Fun {
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
        /*table
                .groupBy($("id"))
                .flatAggregate(call(MyUDATF.class,$("vc")).as("value","top"))
                .select($("id"),$("value"),$("top"))
                .execute()
                .print();*/
        // TODO: 先注册再使用
        tableEnv.createTemporaryFunction("top2", MyUDATF.class);


        //API
        table
                .groupBy($("id"))
                .flatAggregate(call("top2",$("vc")).as("value","top"))
                .select($("id"),$("value"),$("top"))
                .execute()
                .print();

        //SQL 不支持


    }

    public static class myAcc{
        public Integer first;
        public Integer second;
    }

    // TODO: 自定义udaaf函数(vc的最大的两个值)
    public static class MyUDATF extends TableAggregateFunction<Tuple2<Integer
            ,Integer>,myAcc>{
        //初始化
        @Override
        public myAcc createAccumulator() {
            myAcc acc = new myAcc();
            acc.first=Integer.MIN_VALUE;
            acc.second=Integer.MIN_VALUE;
            return acc;
        }

        public void accumulate(myAcc acc,Integer value){
            if (value>acc.first) {
                acc.second=acc.first;
                acc.first=value;
            } else if (value>acc.second){
                acc.second=value;
            }

        }

        public void emitValue(myAcc acc, Collector<Tuple2<Integer,Integer>> out){
            if (acc.first!=Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.first,1));
            }
            if (acc.second!=Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.second,2));
            }
        }
    }

}
