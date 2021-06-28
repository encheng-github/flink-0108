package com.atguigu.day04;

import com.atguigu.day04.bean.OrderEvent;
import com.atguigu.day04.bean.TxEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.HashMap;

public class Flink05_Project_Order {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.设置并行度
        env.setParallelism(1);
        //3.获取数据
        DataStreamSource<String> orderDStream = env.readTextFile("input/OrderLog.csv");
        DataStreamSource<String> txDStream = env.readTextFile("input/ReceiptLog.csv");

        //4.将数据转为JavaBean
        SingleOutputStreamOperator<OrderEvent> orderEventDStream = orderDStream.map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String s) throws Exception {
                //a.对数据做切分
                String[] split = s.split(",");
                return new OrderEvent(
                        Long.parseLong(split[0]),
                        split[1],
                        split[2],
                        Long.parseLong(split[3])
                );
            }
        });

        SingleOutputStreamOperator<TxEvent> txEventDStream = txDStream.map(new MapFunction<String, TxEvent>() {
            @Override
            public TxEvent map(String s) throws Exception {
                //a.对数据做切分
                String[] split = s.split(",");
                return new TxEvent(
                        split[0],
                        split[1],
                        Long.parseLong(split[2])
                );
            }
        });

        //5.将两条流连接起来
        ConnectedStreams<OrderEvent, TxEvent> connect = orderEventDStream.connect(txEventDStream);

        /**
         * 用connect时,必须要用keyby,否则可能会造成数据丢失
         * 先用connect和先用keyby一样
         */

        //6.对数据做keyBy
        ConnectedStreams<OrderEvent, TxEvent> orderEventTxEventConnectedStreams = connect.keyBy("txId", "txId");

        //7.操作数据,进行实时对账
        orderEventTxEventConnectedStreams.process(new CoProcessFunction<OrderEvent, TxEvent, String>() {
            test test=new test();
            //创建hashMap缓存order数据
            HashMap<String, OrderEvent> eventHashMap = new HashMap<>();
            //创建hashMap缓存TX(交易)数据
            HashMap<String, TxEvent> txHashMap = new HashMap<>();

            @Override
            public void processElement1(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
                System.out.println(1111111);
                //1.去对方缓存中看是否有能够关联上的数据
                if (txHashMap.containsKey(value.getTxId())) {
                    //能够关联上
                    System.out.println("订单: " + value.getOrderId() + "对账成功!");
                    //清空缓存
                    txHashMap.remove(value.getTxId());
                }else {
                    //关联不上,把自己存进去
                    eventHashMap.put(value.getTxId(),value);
                }

            }

            @Override
            public void processElement2(TxEvent value, Context ctx, Collector<String> out) throws Exception {
                System.out.println(test);
                //1.去对方缓存中看是否有能够关联上的数据
                if (eventHashMap.containsKey(value.getTxId())) {
                    //能够关联上
                    System.out.println("订单: " + eventHashMap.get(value.getTxId()).getOrderId() + "对账成功!");
                    //清空缓存
                    eventHashMap.remove(value.getTxId());
                }else {
                    //关联不上,把自己存进去
                    txHashMap.put(value.getTxId(),value);
                }
            }
        }).print();
        env.execute();

    }
    public static class test implements Serializable{
        public test(){
            System.out.println("1323");
        }
    }
}
