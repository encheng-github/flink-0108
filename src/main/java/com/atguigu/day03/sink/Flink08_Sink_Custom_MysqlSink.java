package com.atguigu.day03.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.day02.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.List;

public class Flink08_Sink_Custom_MysqlSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> map =
                streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String s) throws Exception {
                String[] split = s.split(" ");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });



        map.addSink(new MysqlSinkCustom());
        env.execute();
    }
    public static class MysqlSinkCustom extends RichSinkFunction<WaterSensor>{
        private Connection connection;
        private PreparedStatement pstm;

        @Override
        public void invoke(WaterSensor value, Context context) throws Exception {


            //3.给占位符赋值
            pstm.setString(1,value.getId());
            pstm.setLong(2,value.getTs());
            pstm.setInt(3,value.getVc());

            pstm.execute();
            System.out.println("invoke...");


        }

        //在open中创建连接,一个并行度只会执行一次,并且会最先执行
        @Override
        public void open(Configuration parameters) throws Exception {
            //1.创建数据链接
            connection = DriverManager.getConnection("jdbc" +
                            ":mysql://hadoop102:3306/test?useSSL=false", "root",
                    "123456");
            //2.创建语句预执行者
            pstm = connection.prepareStatement("insert into sensor values (?,?," +
                    "?)");
        }

        //在close中关闭连接,一个并行度只会执行一次,并且会最后执行
        @Override
        public void close() throws Exception {
            //关闭连接
            pstm.close();
            connection.close();
        }
    }
}
