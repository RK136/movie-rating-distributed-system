package com.movielens.entity; // 包声明：实体类层，用于封装业务数据（前后端传输的载体）


// 纯POJO类（Plain Old Java Object）：无任何框架依赖注解，仅包含字段、getter/setter
// 作用：作为数据容器，封装电影的汇总信息（从HBase查询的结果、返回给前端的数据都用它承载）
public class MovieAgg {
    // 字段设计：与HBase表`movie_aggregate`的列一一对应（确保数据映射无歧义）
    private String movieId;          // 行键（对应HBase的rowkey，即电影唯一ID，如"1"、"2"）
    private String title;           // 电影名（对应HBase的`basic:title`列，如"Toy Story"）
    private String genres;          // 电影类型（对应HBase的`basic:genres`列，如"Animation|Children|Comedy"）
    private String imdbId;          // IMDB平台ID（对应HBase的`external:imdbId`列，如"tt0114709"）
    private Double avgRating;       // 平均评分（对应HBase的`rating_stats:avg_rating`列，如3.9）
    private Integer totalRatings;   // 总评分次数（对应HBase的`rating_stats:total_ratings`列，如5832）
    private String topTag1;         // 热门标签1（对应HBase的`tags:top_tag1`列，如"animation"）


    // 所有字段的getter/setter方法（遵循JavaBean规范，必须存在）
    // 作用：1. 封装字段访问（字段私有，通过方法访问，控制权限）；2. 支持框架反射（如Spring转JSON、前端Vue绑定）

    /**
     * 获取电影ID（行键）
     * @return 电影唯一标识（如"1"）
     */
    public String getMovieId() {
        return movieId;
    }

    /**
     * 设置电影ID
     * @param movieId 从HBase行键解析的字符串（如"1"）
     */
    public void setMovieId(String movieId) {
        this.movieId = movieId;
    }

    /**
     * 获取电影名
     * @return 电影标题（如"Toy Story"）
     */
    public String getTitle() {
        return title;
    }

    /**
     * 设置电影名
     * @param title 从HBase`basic:title`解析的字符串
     */
    public void setTitle(String title) {
        this.title = title;
    }

    /**
     * 获取电影类型
     * @return 类型字符串（如"Animation|Children|Comedy"）
     */
    public String getGenres() {
        return genres;
    }

    /**
     * 设置电影类型
     * @param genres 从HBase`basic:genres`解析的字符串
     */
    public void setGenres(String genres) {
        this.genres = genres;
    }

    /**
     * 获取IMDB ID
     * @return 外部平台标识（如"tt0114709"）
     */
    public String getImdbId() {
        return imdbId;
    }

    /**
     * 设置IMDB ID
     * @param imdbId 从HBase`external:imdbId`解析的字符串
     */
    public void setImdbId(String imdbId) {
        this.imdbId = imdbId;
    }

    /**
     * 获取平均评分
     * @return 评分数值（如3.9）
     */
    public Double getAvgRating() {
        return avgRating;
    }

    /**
     * 设置平均评分
     * @param avgRating 从HBase`rating_stats:avg_rating`解析的Double值（需先转字符串再转数值）
     */
    public void setAvgRating(Double avgRating) {
        this.avgRating = avgRating;
    }

    /**
     * 获取总评分次数
     * @return 评分人数（如5832）
     */
    public Integer getTotalRatings() {
        return totalRatings;
    }

    /**
     * 设置总评分次数
     * @param totalRatings 从HBase`rating_stats:total_ratings`解析的Integer值
     */
    public void setTotalRatings(Integer totalRatings) {
        this.totalRatings = totalRatings;
    }

    /**
     * 获取热门标签1
     * @return 标签字符串（如"animation"）
     */
    public String getTopTag1() {
        return topTag1;
    }

    /**
     * 设置热门标签1
     * @param topTag1 从HBase`tags:top_tag1`解析的字符串
     */
    public void setTopTag1(String topTag1) {
        this.topTag1 = topTag1;
    }
}