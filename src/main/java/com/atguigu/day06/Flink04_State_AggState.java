package com.atguigu.day06;

import com.atguigu.day02.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
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

public class Flink04_State_AggState {
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

        //5.计算每个传感器的平均水位
        keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, Double>() {
            // TODO: 定义aggregate类型的状态用来保存历史结果
            private AggregatingState<Integer,Double> avgState;

            @Override
            public void open(Configuration parameters) throws Exception {
                avgState=getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Integer, Tuple2<Integer,Integer>, Double>("agg-State", new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {
                    //创建累加器
                    @Override
                    public Tuple2<Integer, Integer> createAccumulator() {
                        return Tuple2.of(0,0);
                    }

                    //累加操作
                    @Override
                    public Tuple2<Integer, Integer> add(Integer value,Tuple2<Integer, Integer> accumulator) {
                        return Tuple2.of(accumulator.f0+value,
                                accumulator.f1+1);
                    }

                    //返回结果数据
                    @Override
                    public Double getResult(Tuple2<Integer, Integer> integerIntegerTuple2) {
                        return integerIntegerTuple2.f0*1D/integerIntegerTuple2.f1;
                    }

                    @Override
                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                        return Tuple2.of(a.f0+b.f0,a.f1+b.f1);
                    }
                },Types.TUPLE(Types.INT,Types.INT)));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<Double> out) throws Exception {

                //1.将数据添加至状态
                avgState.add(value.getVc());

                //2.从状态中取出
                Double result = avgState.get();
                out.collect(result);
            }
        }).print();

        env.execute();
    }
}
