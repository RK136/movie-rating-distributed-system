package com.movielens.entity;

import java.io.Serializable;

public class KafkaRatingMessage implements Serializable {
    private String movieId;
    private Double rating;
    private Long timestamp;

    // Getter + Setter（必须与后端一致）
    public String getMovieId() { return movieId; }
    public void setMovieId(String movieId) { this.movieId = movieId; }
    public Double getRating() { return rating; }
    public void setRating(Double rating) { this.rating = rating; }
    public Long getTimestamp() { return timestamp; }
    public void setTimestamp(Long timestamp) { this.timestamp = timestamp; }
}