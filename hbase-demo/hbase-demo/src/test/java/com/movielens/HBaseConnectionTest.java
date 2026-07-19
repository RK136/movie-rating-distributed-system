package com.movielens;

import com.movielens.config.HBaseConfig;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(classes = {HbaseDemoApplication.class, HBaseConfig.class})
public class HBaseConnectionTest {

    // 自动注入HBase连接
    @Autowired
    private Connection hbaseConnection;

    /**D
     * 测试HBase连接是否成功
     */
    @Test
    public void testConnection() {
        // 若能打印连接对象，说明连接成功
        System.out.println("HBase连接对象信息：" + hbaseConnection);
        // 额外验证：检查连接是否有效
        assert hbaseConnection != null : "HBase连接为空，创建失败！";
        assert !hbaseConnection.isClosed() : "HBase连接已关闭！";
        System.out.println("✅ HBase连接测试成功！");
    }
}