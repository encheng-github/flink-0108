package com.atguigu.day03.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.day02.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.Arrays;
import java.util.List;

public class Flink07_Sink_ES_Sink {
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

        // TODO: ES_Sink
        //创建list集合用来存放Httphost对象,用来指定节点名和端口号
        List<HttpHost> httpHosts = Arrays.asList(
                new HttpHost("hadoop102", 9200),
                new HttpHost("hadoop103", 9200),
                new HttpHost("hadoop104", 9200)
        );

        ElasticsearchSink.Builder<WaterSensor> waterSensorBuilder = new ElasticsearchSink.Builder<>(httpHosts,
                new ElasticsearchSinkFunction<WaterSensor>() {
            //指定往ES中哪个索引哪个类型哪个id中写入什么数据,并且写入的数据需要kv类型,否则报错
                    @Override
                    public void process(WaterSensor element,
                                        RuntimeContext ctx,
                                        RequestIndexer indexer) {
                        IndexRequest indexRequest = new IndexRequest("sensor0108", "_doc", element.getId())
                                //注意source中需要的kv类型的数据
                                .source(JSON.toJSON(element),
                                        XContentType.JSON);
                        indexer.add(indexRequest);
                    }
                });

        //因为读的是无界数据,ES自身需要将数据积攒到一块然后写入,所以设置参数
        waterSensorBuilder.setBulkFlushMaxActions(1);
        ElasticsearchSink<WaterSensor> elasticsearchSink = waterSensorBuilder.build();
        map.addSink(elasticsearchSink);
        env.execute();
    }
}
