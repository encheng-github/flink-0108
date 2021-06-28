package com.atguigu.day06;

import com.atguigu.day02.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
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

public class Flink02_State_ListState {
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

        //5.针对每个传感器输出最高的3个水位值
        keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor,
                List<Integer>>() {
            // TODO: 1.使用listState保存多条状态数据
            private ListState<Integer> listVc;

            @Override
            public void open(Configuration parameters) throws Exception {
                // TODO: 2.初始化数据状态
                listVc=getRuntimeContext().getListState(new ListStateDescriptor<Integer>("list-state", Types.INT));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<List<Integer>> out) throws Exception {
                //先将数据保存至状态中
                listVc.add(value.getVc());

                //创建list集合用来保存状态中的数据
                ArrayList<Integer> vCs = new ArrayList<>();

                //取出状态中的数据并放入保存结果的list集合中
                Iterable<Integer> vCsState = listVc.get();
                for (Integer vc : vCsState) {
                    vCs.add(vc);
                }

                //对保存结果的list集合排序
                vCs.sort(new Comparator<Integer>() {
                    @Override
                    public int compare(Integer o1, Integer o2) {
                        return o2-o1;
                    }
                });

                //只取出最高的三个水位
                if (vCs.size()>3) {
                    vCs.remove(3);
                }

                //将结果集合中的数据更新至状态中
                listVc.update(vCs);

                //将结果集合发送到下游
                out.collect(vCs);
            }
        }).print();

        env.execute();
    }
}
