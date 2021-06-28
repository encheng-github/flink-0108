package com.atguigu.day06;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class Flink08_State_Operator_Broadcast {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //从两个端口读取数据,分别对应两条流
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);
        DataStreamSource<String> streamSource1 = env.socketTextStream("hadoop102", 8888);

        //将streamSource1来的流广播出去,获取广播流
        MapStateDescriptor<String, String> mapStateDescriptor =
                new MapStateDescriptor<String, String>("broadcast-state",String.class,String.class);

        BroadcastStream<String> broadcastStream = streamSource1.broadcast(mapStateDescriptor);

        //将两条流连接起来
        BroadcastConnectedStream<String, String> connect = streamSource.connect(broadcastStream);

        connect.process(new BroadcastProcessFunction<String, String, String>() {
            @Override
            public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {

                //提取状态
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                if ("1".equals(broadcastState.get("switch"))) {
                    out.collect("执行1逻辑....");
                }else if ("2".equals(broadcastState.get("switch"))){
                    out.collect("执行2逻辑....");
                }else {
                    out.collect("执行其他逻辑....");
                }
            }

            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {

                //提取状态
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

                broadcastState.put("switch",value);
            }
        }).print();

        env.execute();
    }
}
