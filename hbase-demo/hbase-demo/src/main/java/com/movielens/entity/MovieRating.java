package com.movielens.entity;

import java.io.Serializable;

// 必须实现 Serializable（Kafka序列化要求）
public class MovieRating implements Serializable {
    private String movieId; // 电影ID
    private double rating;  // 评分（1-5分）
    private long timestamp; // 时间戳（毫秒）

    // Getter + Setter
    public String getMovieId() { return movieId; }
    public void setMovieId(String movieId) { this.movieId = movieId; }
    public double getRating() { return rating; }
    public void setRating(double rating) { this.rating = rating; }
    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
}