package com.movielens.utils;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

/**
 * 电影评分缓存工具类：
 * 1. 内存实时计算平均分和评分人数（查询快）；
 * 2. 同步写入HBase（数据不丢失）；
 * 3. 项目启动时加载HBase历史数据到内存（重启后不丢失）。
 */
@Component
public class MovieRatingCache {

    // 内存缓存结构：key=movieId，value=[总评分人数（索引0）、总评分之和（索引1）]
    private static final Map<String, double[]> RATING_CACHE = new HashMap<>();

    /**
     * 项目启动时执行（Spring注解）：从HBase加载历史评分数据到内存
     * 作用：重启后端后，内存中仍有之前的评分数据，前端展示正常
     */
    @PostConstruct
    public void initCacheFromHBase() {
        try (
                // 复用HBase工具类获取连接（try-with-resources自动关闭资源，无需手动close）
                Connection hbaseConn = HBaseClientUtil.getConnection();
                Table movieTable = hbaseConn.getTable(TableName.valueOf("movie_aggregate"));
                // 扫描HBase中所有电影的评分数据（仅扫描rating_stats列族，高效）
                ResultScanner scanner = movieTable.getScanner(new org.apache.hadoop.hbase.client.Scan().addFamily(Bytes.toBytes("rating_stats")))
        ) {
            int loadCount = 0;
            for (org.apache.hadoop.hbase.client.Result result : scanner) {
                // 1. 获取电影ID（HBase行键=movieId）
                String movieId = Bytes.toString(result.getRow());

                // 2. 读取HBase中的评分人数（rating_stats:total_ratings）
                byte[] totalRatingsBytes = result.getValue(
                        Bytes.toBytes("rating_stats"),
                        Bytes.toBytes("total_ratings")
                );
                // 3. 读取HBase中的平均分（rating_stats:avg_rating）
                byte[] avgRatingBytes = result.getValue(
                        Bytes.toBytes("rating_stats"),
                        Bytes.toBytes("avg_rating")
                );

                // 4. 解析数据并存入内存（总分=平均分×人数）
                if (totalRatingsBytes != null && avgRatingBytes != null) {
                    int totalRatings = Integer.parseInt(Bytes.toString(totalRatingsBytes));
                    double avgRating = Double.parseDouble(Bytes.toString(avgRatingBytes));
                    double totalSum = avgRating * totalRatings; // 计算总分，用于后续增量计算
                    RATING_CACHE.put(movieId, new double[]{totalRatings, totalSum});
                    loadCount++;
                }
            }
            System.out.println("初始化完成：从HBase加载" + loadCount + "部电影的评分数据到内存");
        } catch (Exception e) {
            System.err.println("初始化评分缓存失败！错误信息：" + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 核心方法：接收新评分，实时计算+同步写HBase
     * @param movieId 被评分的电影ID
     * @param newRating 新评分（1-5分）
     * @return 更新后的结果（movieId、avgRating、totalRatings）
     */
    public Map<String, Object> updateRating(String movieId, double newRating) {
        // 1. 内存实时计算（增量更新，效率O(1)）
        RATING_CACHE.putIfAbsent(movieId, new double[]{0, 0}); // 电影首次评分时初始化
        double[] cacheData = RATING_CACHE.get(movieId);
        int oldTotalRatings = (int) cacheData[0]; // 原评分人数
        double oldTotalSum = cacheData[1]; // 原评分总和

        int newTotalRatings = oldTotalRatings + 1; // 新评分人数=原人数+1
        double newTotalSum = oldTotalSum + newRating; // 新总分=原总分+新评分
        double newAvgRating = Math.round((newTotalSum / newTotalRatings) * 10) / 10.0; // 新平均分（保留1位小数）

        // 更新内存缓存
        RATING_CACHE.put(movieId, new double[]{newTotalRatings, newTotalSum});

        // 2. 同步写入HBase（确保数据持久化，不丢失）
        writeToHBase(movieId, newTotalRatings, newAvgRating);

        // 3. 组装更新结果（用于返回前端和WebSocket推送）
        Map<String, Object> updateResult = new HashMap<>();
        updateResult.put("movieId", movieId);
        updateResult.put("totalRatings", newTotalRatings);
        updateResult.put("avgRating", newAvgRating);
        return updateResult;
    }

    /**
     * 辅助方法：将最新评分数据写入HBase
     */
    private void writeToHBase(String movieId, int totalRatings, double avgRating) {
        try (
                Connection hbaseConn = HBaseClientUtil.getConnection();
                Table movieTable = hbaseConn.getTable(TableName.valueOf("movie_aggregate"))
        ) {
            // 1. 构建Put对象（HBase更新操作：行键=movieId）
            Put put = new Put(Bytes.toBytes(movieId));

            // 2. 写入评分人数（rating_stats:total_ratings）
            put.addColumn(
                    Bytes.toBytes("rating_stats"), // 列族
                    Bytes.toBytes("total_ratings"), // 列名
                    Bytes.toBytes(String.valueOf(totalRatings)) // 列值（转为字符串存储，避免类型错误）
            );

            // 3. 写入平均分（rating_stats:avg_rating）
            put.addColumn(
                    Bytes.toBytes("rating_stats"),
                    Bytes.toBytes("avg_rating"),
                    Bytes.toBytes(String.valueOf(avgRating))
            );

            // 4. 执行写入HBase（同步操作，写成功才返回）
            movieTable.put(put);
            System.out.println("HBase写入成功：movieId=" + movieId + "，新评分人数=" + totalRatings + "，新平均分=" + avgRating);
        } catch (Exception e) {
            // 写入失败仅打印日志，不影响内存更新（保证前端实时性，后续可手动补数据）
            System.err.println("HBase写入失败！movieId=" + movieId + "，错误信息：" + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 对外提供：根据movieId查询实时评分数据（从内存取，快）
     */
    public Map<String, Object> getRealTimeRating(String movieId) {
        double[] cacheData = RATING_CACHE.getOrDefault(movieId, new double[]{0, 0});
        Map<String, Object> ratingData = new HashMap<>();
        ratingData.put("totalRatings", (int) cacheData[0]);
        ratingData.put("avgRating", cacheData[0] == 0 ? 0.0 : Math.round((cacheData[1]/cacheData[0])*10)/10.0);
        return ratingData;
    }
}