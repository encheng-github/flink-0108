package com.atguigu.view;

import com.atguigu.view.bean.TableA;
import com.atguigu.view.bean.TableB;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class Flink12_Sql_Join {
    public static void main(String[] args) {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //设置sql中状态的保存时间
        System.out.println(tableEnv.getConfig().getIdleStateRetention());
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        //2.读取端口数据创建流
        SingleOutputStreamOperator<TableA> ds1 = env.socketTextStream(
                "hadoop102", 8888).map(line -> {
            String[] split = line.split(",");
            return new TableA(split[0], split[1]);
        });
        SingleOutputStreamOperator<TableB> ds2 = env.socketTextStream(
                "hadoop102", 9999).map(line -> {
            String[] split = line.split(",");
            return new TableB(split[0], split[1]);
        });

        //3.创建动态表
        Table t1 = tableEnv.fromDataStream(ds1);
        tableEnv.createTemporaryView("t1",t1);

        Table t2 = tableEnv.fromDataStream(ds2);
        tableEnv.createTemporaryView("t2",t2);

        //4.双流join(到时间就关闭) 左表:OnCreateAndWrite 右表:OnCreateAndWrite
       /* TableResult tableResult = tableEnv.executeSql("select t1.id,t1.name,t2.sex from t1 join t2 on t1.id=t2.id");*/

        //left join 左表:OnReadAndWrite 右表:OnCreateAndWrite
        TableResult tableResult1 = tableEnv.executeSql("select t1.id,t1.name," +
                "t2.sex from t1 left join t2 on t1.id=t2.id");

//        //right join 左表:OnCreateAndWrite 右表:OnReadAndWrite
//        TableResult tableResult2 = tableEnv.executeSql("select t1.id,t1.name," +
//                "t2.sex from t1 right join t2 on t1.id=t2.id");
//        //full join 左表:OnReadAndWrite 右表:OnReadAndWrite
//        TableResult tableResult3 = tableEnv.executeSql("select t1.id,t1.name," +
//                "t2.sex from t1 full join t2 on t1.id=t2.id");

        //5.打印结果
        tableResult1.print();

        //6.启动
    }
}
