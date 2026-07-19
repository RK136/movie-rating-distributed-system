package com.movielens.service;

import com.movielens.entity.MovieAgg;
import com.movielens.utils.HBaseClientUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
public class FullMovieService {

    private static final String TABLE_NAME = "movie_aggregate";

    // 按关键词查询电影汇总数据（补全所有字段读取）
    public List<MovieAgg> queryMoviesByKeyword(String keyword) throws IOException {
        Connection conn = HBaseClientUtil.getConnection();
        Table table = conn.getTable(TableName.valueOf(TABLE_NAME));

        // 构造前缀过滤器（假设行键是movieId，或按title前缀过滤，根据你的表设计调整）
        Scan scan = new Scan();
        scan.setFilter(new PrefixFilter(Bytes.toBytes(keyword))); // 按关键词前缀匹配

        // 显式添加所有需要读取的列（核心：确保每个字段都被读取）
        scan.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("title"));       // 电影名
        scan.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("genres"));      // 类型
        scan.addColumn(Bytes.toBytes("external"), Bytes.toBytes("imdbId"));   // IMDB ID
        scan.addColumn(Bytes.toBytes("rating_stats"), Bytes.toBytes("avg_rating")); // 平均评分
        scan.addColumn(Bytes.toBytes("tags"), Bytes.toBytes("top_tag1"));     // 热门标签

        ResultScanner scanner = table.getScanner(scan);
        List<MovieAgg> resultList = new ArrayList<>();

        for (Result result : scanner) {
            MovieAgg movieAgg = new MovieAgg();
            // 行键（movieId）
            movieAgg.setMovieId(Bytes.toString(result.getRow()));

            // 电影名（basic:title）
            if (result.containsColumn(Bytes.toBytes("basic"), Bytes.toBytes("title"))) {
                movieAgg.setTitle(Bytes.toString(result.getValue(
                        Bytes.toBytes("basic"), Bytes.toBytes("title")
                )));
            }

            // 类型（basic:genres）
            if (result.containsColumn(Bytes.toBytes("basic"), Bytes.toBytes("genres"))) {
                movieAgg.setGenres(Bytes.toString(result.getValue(
                        Bytes.toBytes("basic"), Bytes.toBytes("genres")
                )));
            }

            // IMDB ID（external:imdbId）
            if (result.containsColumn(Bytes.toBytes("external"), Bytes.toBytes("imdbId"))) {
                movieAgg.setImdbId(Bytes.toString(result.getValue(
                        Bytes.toBytes("external"), Bytes.toBytes("imdbId")
                )));
            }

            // 平均评分（rating_stats:avg_rating）
            if (result.containsColumn(Bytes.toBytes("rating_stats"), Bytes.toBytes("avg_rating"))) {
                movieAgg.setAvgRating(Bytes.toDouble(result.getValue(
                        Bytes.toBytes("rating_stats"), Bytes.toBytes("avg_rating")
                )));
            }

            // 热门标签（tags:top_tag1）
            if (result.containsColumn(Bytes.toBytes("tags"), Bytes.toBytes("top_tag1"))) {
                movieAgg.setTopTag1(Bytes.toString(result.getValue(
                        Bytes.toBytes("tags"), Bytes.toBytes("top_tag1")
                )));
            }

            resultList.add(movieAgg);
        }

        scanner.close();
        table.close();
        return resultList;
    }
}