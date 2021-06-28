package com.atguigu.day04;

import com.atguigu.day04.bean.MarketingUserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class Flink03_Project_AppAnalysis_By_Chanel {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<MarketingUserBehavior> streamSource = env.addSource(new AppMarketingDataSource());

        //1.将数据转为Tuple格式(渠道+行为,1L)
        SingleOutputStreamOperator<Tuple2<String, Long>> chanelWithBehaviorsToOneDStream = streamSource.flatMap(new FlatMapFunction<MarketingUserBehavior,
                Tuple2<String, Long>>() {
            @Override
            public void flatMap(MarketingUserBehavior value,
                                Collector<Tuple2<String, Long>> out) throws Exception {
                out.collect(Tuple2.of(value.getChannel() + "_" + value.getBehavior(), 1L));
            }
        });
        //2.将相同key的数据聚合到一起
        KeyedStream<Tuple2<String, Long>, Tuple> keyedStream = chanelWithBehaviorsToOneDStream.keyBy(0);

        //3.对同一渠道同一行为的数据做累加
        keyedStream.sum(1).print();
        env.execute();

    }
    public static class AppMarketingDataSource extends RichSourceFunction<MarketingUserBehavior> {
        boolean canRun = true;
        Random random = new Random();
        List<String> channels = Arrays.asList("huawwei", "xiaomi", "apple", "baidu", "qq", "oppo", "vivo");
        List<String> behaviors = Arrays.asList("download", "install", "update", "uninstall");

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            while (canRun) {
                MarketingUserBehavior marketingUserBehavior = new MarketingUserBehavior(
                        (long) random.nextInt(1000000),
                        behaviors.get(random.nextInt(behaviors.size())),
                        channels.get(random.nextInt(channels.size())),
                        System.currentTimeMillis());
                ctx.collect(marketingUserBehavior);
                Thread.sleep(200);
            }
        }

        @Override
        public void cancel() {
            canRun = false;
        }
    }

}
