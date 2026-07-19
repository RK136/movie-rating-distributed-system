package com.movielens.entity;

import java.util.List;
import java.util.Map;

public class FullMovieInfo {
    // 1. 声明私有字段
    private String movieId;
    private String title;
    private String genres;
    private String imdbId;
    private String tmdbId;
    private Double avgRating;
    private Integer totalRatings;
    private Double latestRating;
    private Integer totalTags;
    private List<String> topTags;

    // 2. 无参构造函数（必要时可添加有参构造）
    public FullMovieInfo() {}

    // 3. 手动编写getter和setter方法
    public String getMovieId() {
        return movieId;
    }

    public void setMovieId(String movieId) {
        this.movieId = movieId;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getGenres() {
        return genres;
    }

    public void setGenres(String genres) {
        this.genres = genres;
    }

    public String getImdbId() {
        return imdbId;
    }

    public void setImdbId(String imdbId) {
        this.imdbId = imdbId;
    }

    public String getTmdbId() {
        return tmdbId;
    }

    public void setTmdbId(String tmdbId) {
        this.tmdbId = tmdbId;
    }

    public Double getAvgRating() {
        return avgRating;
    }

    public void setAvgRating(Double avgRating) {
        this.avgRating = avgRating;
    }

    public Integer getTotalRatings() {
        return totalRatings;
    }

    public void setTotalRatings(Integer totalRatings) {
        this.totalRatings = totalRatings;
    }

    public Double getLatestRating() {
        return latestRating;
    }

    public void setLatestRating(Double latestRating) {
        this.latestRating = latestRating;
    }

    public Integer getTotalTags() {
        return totalTags;
    }

    public void setTotalTags(Integer totalTags) {
        this.totalTags = totalTags;
    }

    public List<String> getTopTags() {
        return topTags;
    }

    public void setTopTags(List<String> topTags) {
        this.topTags = topTags;
    }
}