package com.atguigu.day06;

import com.atguigu.day02.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class Flink03_State_ReduceState {
    public static void main(String[] args) throws Exception {

        //1.创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //3.将数据转为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String s) throws Exception {
                String[] split = s.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]),
                        Integer.parseInt(split[2]));
            }
        });

        //4.对数据做keyby
        KeyedStream<WaterSensor, Tuple> keyedStream = waterSensorDStream.keyBy("id");

        //5.计算每个传感器的水位和
        keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, Tuple2<String,Integer>>() {
            // TODO: 设置reduce类型的状态来保存历史数据
            private ReducingState<Tuple2<String,Integer>> vcSumState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //初始化状态
                vcSumState=
                        getRuntimeContext().getReducingState(new ReducingStateDescriptor<Tuple2<String, Integer>>("reducing-State", new ReduceFunction<Tuple2<String, Integer>>() {
                            @Override
                            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                                return Tuple2.of(value1.f0,value1.f1+value2.f1);
                            }
                        },Types.TUPLE(Types.STRING,Types.INT)));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

                //1.保存状态
                vcSumState.add(Tuple2.of(value.getId(),value.getVc()));

                //2.获取状态
                Tuple2<String, Integer> result = vcSumState.get();

                //3.将结果发送至下游
                out.collect(result);

            }
        }).print();

        env.execute();
    }
}
