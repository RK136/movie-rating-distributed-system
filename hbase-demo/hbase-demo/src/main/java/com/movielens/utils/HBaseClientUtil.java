// 包路径：com.movielens.utils（工具类统一存放路径，便于管理）
package com.movielens.utils;

// 导入HBase配置、连接相关的核心类
import org.apache.hadoop.conf.Configuration; // Hadoop/HBase配置对象（存储连接参数）
import org.apache.hadoop.hbase.HBaseConfiguration; // HBase配置创建工具类
import org.apache.hadoop.hbase.client.Connection; // HBase连接接口（客户端与集群的连接句柄）
import org.apache.hadoop.hbase.client.ConnectionFactory; // HBase连接工厂（用于创建连接）

import java.io.IOException; // IO异常（连接创建/关闭可能抛出）


/**
 * HBase客户端工具类
 * 核心作用：管理HBase连接的创建、复用和关闭，
 * 采用单例模式确保全局只有一个连接（减少连接开销，提升性能）
 * 是所有HBase操作（如查询、写入）的基础工具
 */
public class HBaseClientUtil {
    // 🔴 关键配置：ZooKeeper服务器IP（必须与虚拟机实际IP一致，否则连接失败）
    // HBase依赖ZooKeeper管理集群元数据（如RegionServer位置、表结构），必须正确配置
    private static final String ZK_IP = "192.168.56.101";  // 虚拟机中ZooKeeper所在IP（例如你的bigdata节点IP）

    // 🔴 关键配置：ZooKeeper端口（HBase默认使用2181，若你的集群改了端口需同步修改）
    // 注意：若HBase用内置ZK，端口通常是2181；若用独立ZK，需与zkServer.sh的配置一致
    private static final String ZK_PORT = "2182"; // 你的ZooKeeper服务端口

    // HBase连接对象（全局唯一，单例模式的核心变量）
    // Connection是HBase客户端的核心接口，封装了与集群的通信逻辑
    private static Connection conn;

    /**
     * 单例模式获取HBase连接（核心方法）
     * 确保全局只有一个Connection实例，避免频繁创建连接导致的性能损耗
     * @return HBase连接对象（Connection），用于后续操作表
     * @throws IOException 连接创建失败时抛出（如ZK地址错误、HBase未启动）
     */
    public static Connection getConnection() throws IOException {
        // 单例判断：若连接为空，或已关闭，则创建新连接
        // 为什么要判断conn.isClosed()？因为连接可能因超时/异常被关闭，需重新创建
        if (conn == null || conn.isClosed()) {
            // 1. 创建HBase配置对象：加载HBase默认配置（hbase-site.xml）
            // HBaseConfiguration.create()会自动读取classpath下的hbase-site.xml配置
            Configuration config = HBaseConfiguration.create();

            // 2. 配置ZooKeeper地址（覆盖默认配置，指定实际虚拟机ZK地址）
            // HBase客户端必须通过ZK找到HMaster和RegionServer，这是连接的核心参数
            config.set("hbase.zookeeper.quorum", ZK_IP);

            // 3. 配置ZooKeeper端口（与虚拟机ZK服务端口一致）
            config.set("hbase.zookeeper.property.clientPort", ZK_PORT);

            // 4. 禁用Kerberos安全认证（非安全集群必加，否则会报认证错误）
            // 我们的集群是开发环境，未启用Kerberos（分布式系统的安全认证机制），需显式禁用
            config.set("hbase.security.authentication", "simple");

            // 5. 通过连接工厂创建连接（核心：建立客户端与HBase集群的连接）
            // 此过程会与ZK通信，获取集群元数据，耗时较长（3-5秒），因此必须单例复用
            conn = ConnectionFactory.createConnection(config);
        }
        // 返回已创建的连接（复用已有连接，避免重复创建）
        return conn;
    }

    /**
     * 关闭HBase连接（程序退出时调用）
     * 释放连接资源，避免HBase集群连接池耗尽
     * @throws IOException 连接关闭失败时抛出
     */
    public static void closeConnection() throws IOException {
        // 安全关闭：仅当连接不为空且未关闭时，才执行关闭
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }
}