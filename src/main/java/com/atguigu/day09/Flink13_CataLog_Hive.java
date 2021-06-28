package com.atguigu.day09;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class Flink13_CataLog_Hive {
    public static void main(String[] args) {
        // TODO: 设置用户权限
        System.setProperty("HADOOP_USER_NAME","atguigu");

        //1.流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO: 创建catalog
        String catalogName = "hiveCatalog";
        String databaseName = "flink_test";
        String hiveConfDir = "D:\\BigData\\code\\BJ_Flink\\hive";

        HiveCatalog hiveCatalog = new HiveCatalog(catalogName, databaseName, hiveConfDir);

        // TODO: 注册cataLog
        tableEnv.registerCatalog(catalogName,hiveCatalog);

        // TODO: 设置使用的cataLog以及库(hive)名
        tableEnv.useCatalog(catalogName);
        tableEnv.useDatabase(databaseName);

        // TODO: 设置方sql言
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        // TODO:  查询hive中的表数据
        tableEnv.executeSql("select * from stu").print();

    }
}
