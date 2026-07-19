package com.movielens.kafka;

import com.alibaba.fastjson.JSON;
import com.movielens.entity.KafkaRatingMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Kafka 生产者：接收前端评分，发送到 Kafka 主题（movie-rating-topic）
 */
@Component
public class KafkaRatingProducer {

    // Spring 自动注入 Kafka 模板（简化消息发送）
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    // 目标 Kafka 主题（与步骤2创建的一致）
    private static final String RATING_TOPIC = "movie-rating-topic";

    /**
     * 发送评分消息到 Kafka
     * @param message 评分消息实体（movieId + rating + timestamp）
     */
    public void sendRatingMessage(KafkaRatingMessage message) {
        try {
            // 1. 将实体序列化为 JSON 字符串（Kafka 仅支持字符串/字节数组传输）
            String jsonMessage = JSON.toJSONString(message);
            // 2. 发送消息（key=movieId，让同一电影的评分进入同一分区，提升计算效率）
            kafkaTemplate.send(RATING_TOPIC, message.getMovieId(), jsonMessage);
            // 3. 日志打印（调试用）
            System.out.printf("✅ Kafka 消息发送成功：movieId=%s，评分=%.1f\n",
                    message.getMovieId(), message.getRating());
        } catch (Exception e) {
            // 4. 异常处理（消息发送失败）
            System.err.printf("❌ Kafka 消息发送失败：movieId=%s，错误=%s\n",
                    message.getMovieId(), e.getMessage());
            e.printStackTrace();
        }
    }
}