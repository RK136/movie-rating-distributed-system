package com.movielens.config;

// 只保留Spring的@Configuration注解的导入（用于类上的注解）
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.io.IOException;

// 导入HBase相关类（但不单独导入Hadoop的Configuration，避免冲突）
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/**
 * HBase配置类
 * 使用Spring的@Configuration注解来标识这是一个配置类
 */
// 这里的@Configuration是Spring的注解（正确，无歧义）
@Configuration
public class HBaseConfig {

    // 替换为你的虚拟机IP
    private static final String ZOOKEEPER_QUORUM = "192.168.56.101";
    private static final String ZOOKEEPER_PORT = "2182";

    @Bean(destroyMethod = "close")
    public Connection hbaseConnection() throws IOException {
        // 关键：用全类名声明Hadoop的Configuration对象（彻底消除歧义）
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();

        // 配置参数（不变）
        config.set("hbase.security.authentication", "simple");
        config.set("hadoop.security.authentication", "simple");
        config.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM);
        config.set("hbase.zookeeper.property.clientPort", ZOOKEEPER_PORT);
        config.set("hbase.client.operation.timeout", "60000");
        config.set("hbase.rpc.timeout", "60000");

        return ConnectionFactory.createConnection(config);
    }
}