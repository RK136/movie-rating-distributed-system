package com.movielens.kafka;

import com.alibaba.fastjson.JSON;
import com.movielens.websocket.WebSocketServer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Kafka 消费者：监听 Flink 计算结果（movie-rating-update-topic）→ 推送 WebSocket 给前端
 */
@Component
public class KafkaRatingUpdateConsumer {

    /**
     * 监听 Kafka 主题，接收 Flink 计算后的评分结果
     * @param jsonData 序列化后的计算结果（JSON 字符串）
     * @param acknowledgment 偏移量确认对象（手动提交）
     */
    @KafkaListener(topics = "movie-rating-update-topic", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeRatingUpdate(String jsonData, Acknowledgment acknowledgment) {
        try {
            // 1. 反序列化 JSON 数据→Map（包含 movieId、avgRating、totalRatings、currentRating）
            Map<String, Object> updateData = JSON.parseObject(jsonData, Map.class);
            System.out.printf("✅ 接收 Flink 计算结果：%s\n", updateData);

            // 2. 调用原有 WebSocket 工具类，推送更新到前端（复用原有逻辑，不修改）
            WebSocketServer.pushRatingUpdate(updateData);

            // 3. 手动提交偏移量（确保消息已处理完成，避免重复消费）
            acknowledgment.acknowledge();
            System.out.println("✅ WebSocket 推送成功，已提交 Kafka 偏移量");

        } catch (Exception e) {
            System.err.printf("❌ 处理 Flink 计算结果失败：%s\n", e.getMessage());
            e.printStackTrace();
            // 异常时不提交偏移量，Kafka 会重新推送消息（保证数据不丢失）
        }
    }
}