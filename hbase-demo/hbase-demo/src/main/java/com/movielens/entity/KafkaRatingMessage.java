package com.movielens.entity;

import java.io.Serializable;

/**
 * Kafka 评分消息载体（必须实现 Serializable，支持网络传输）
 */
public class KafkaRatingMessage implements Serializable {
    private String movieId;   // 电影ID（与 HBase 行键一致）
    private Double rating;    // 评分（1-5分）
    private Long timestamp;   // 时间戳（毫秒，用于去重）

    // Getter + Setter（FastJSON 序列化/反序列化必需）
    public String getMovieId() { return movieId; }
    public void setMovieId(String movieId) { this.movieId = movieId; }
    public Double getRating() { return rating; }
    public void setRating(Double rating) { this.rating = rating; }
    public Long getTimestamp() { return timestamp; }
    public void setTimestamp(Long timestamp) { this.timestamp = timestamp; }
}