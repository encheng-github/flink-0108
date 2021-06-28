package com.atguigu.day06;

import com.atguigu.day02.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink01_State_ValueState {
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

        //5.检测传感器的水位线值，如果连续的两个水位线差值超过10，就输出报警
        keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, String>() {
            // TODO: 1.定义状态---用来保存上一条数据
            private ValueState<Integer> lastVc;

            @Override
            public void open(Configuration parameters) throws Exception {
                // TODO: 2.初始化状态
                lastVc=
                        getRuntimeContext().getState(new ValueStateDescriptor<Integer>("value-state",Integer.class));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {

                //获取保存上一次水位的状态
                Integer lastVcState = lastVc.value()==null?0:lastVc.value();
                if (Math.abs(value.getVc()-lastVcState)>10) {
                    out.collect("警报!!!: "+ctx.getCurrentKey());
                }
                //当前的水位保存到状态中
                lastVc.update(value.getVc());
            }
        }).print();
        env.execute();
    }
}
