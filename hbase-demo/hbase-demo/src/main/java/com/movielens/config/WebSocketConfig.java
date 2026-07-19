package com.movielens.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.server.standard.ServerEndpointExporter;

/**
 * WebSocket配置类：启用WebSocket功能（Spring Boot自动扫描@ServerEndpoint注解）
 */
@Configuration
public class WebSocketConfig {

    /**
     * 注册ServerEndpointExporter，让Spring管理WebSocket端点
     */
    @Bean
    public ServerEndpointExporter serverEndpointExporter() {
        return new ServerEndpointExporter();
    }
}