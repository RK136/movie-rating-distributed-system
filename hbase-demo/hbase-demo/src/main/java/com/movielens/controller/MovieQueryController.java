package com.movielens.controller; // 包声明：控制器层，负责接收前端请求

// 导入依赖类：实体类、HBase工具类、HBase核心API、Spring注解等
import com.movielens.entity.KafkaRatingMessage;
import com.movielens.entity.MovieAgg; // 电影汇总信息实体类（存储查询结果）
import com.movielens.entity.MovieRating; // 新增：电影评分实体类（用于Kafka消息传输）
import com.movielens.kafka.KafkaRatingProducer;
import com.movielens.utils.HBaseClientUtil; // HBase连接工具类（单例模式管理连接）
import com.movielens.websocket.WebSocketServer;
import org.apache.hadoop.hbase.Cell; // HBase单元格（存储列族+列名+值）
import org.apache.hadoop.hbase.CellUtil; // HBase单元格工具类（解析单元格数据）
import org.apache.hadoop.hbase.CompareOperator; // HBase比较运算符（如EQUAL、GREATER）
import org.apache.hadoop.hbase.TableName; // HBase表名封装类
import org.apache.hadoop.hbase.client.*; // HBase客户端核心类（连接、表、扫描器等）
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter; // 单列值过滤器（筛选符合条件的行）
import org.apache.hadoop.hbase.filter.SubstringComparator; // 子串比较器（实现模糊匹配）
import org.apache.hadoop.hbase.util.Bytes; // HBase字节数组工具类（字节与字符串/数值互转）
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate; // 新增：Kafka模板（发送消息）
import org.springframework.web.bind.annotation.*; // SpringMVC注解（请求映射、跨域等）

import java.io.IOException; // IO异常（HBase操作可能抛出）
import java.util.ArrayList; // 动态数组（存储查询结果列表）
import java.util.HashMap;
import java.util.List; // 列表接口
import java.util.Map;


/**
 * 电影查询控制器：对接前端，查询movie_aggregate汇总表
 * 核心接口：/api/movie/search?title=关键词（支持模糊匹配）
 * 功能：接收前端电影名查询请求，通过HBase查询数据并返回结构化结果
 */
@RestController // 标识为RESTful控制器：自动将返回值转为JSON，无需额外配置
@RequestMapping("/api/movie") // 设置基础请求路径：所有接口URL前缀为/api/movie
@CrossOrigin // 允许跨域请求：解决前端（如localhost:5173）与后端（localhost:8080）端口不同导致的请求拦截
public class MovieQueryController { // 电影查询控制器类（核心业务逻辑）

    // -------------- 核心依赖注入（删除MovieRatingCache，无需内存缓存）--------------
    // 注入Kafka模板（用于发送评分消息到虚拟机Kafka）
    @Autowired
    private KafkaRatingProducer kafkaRatingProducer;
    // -------------- 常量定义（与虚拟机环境一致）--------------
    // HBase汇总表表名：与虚拟机中创建的表名完全一致（必须相同，否则查不到表）
    private static final TableName MOVIE_AGG_TABLE = TableName.valueOf("movie_aggregate");

    // Kafka主题名：与虚拟机中创建的主题完全一致（存储前端评分消息）
    private static final String KAFKA_RATING_TOPIC = "movie-rating-topic";

    /**
     * 按电影名模糊查询（核心接口）- 步骤7修改版：直接从HBase读取Flink计算后的最新评分
     * @param title 前端输入的电影名关键词（如"Toy"匹配"Toy Story"）
     * @return 匹配的电影汇总数据列表（含标题、类型、Flink计算后的评分等）
     */
    @GetMapping("/search") // 处理GET请求：映射到/api/movie/search路径，前端通过该URL调用
    public List<MovieAgg> searchMovieByTitle(@RequestParam String title) {
        // 初始化电影列表：存储最终要返回给前端的查询结果
        List<MovieAgg> movieList = new ArrayList<>();
        Connection hbaseConn = null;   // HBase连接（需关闭，避免资源泄漏）
        Table table = null;            // HBase表对象（需关闭，避免资源泄漏）
        ResultScanner scanner = null;  // HBase结果扫描器（需关闭，避免资源泄漏）

        try {
            // 1. 获取HBase连接：复用工具类的单例连接（全局只创建1次）
            hbaseConn = HBaseClientUtil.getConnection();
            // 2. 获取汇总表的表对象：通过连接获取操作movie_aggregate表的句柄
            table = hbaseConn.getTable(MOVIE_AGG_TABLE);

            // 3. 构建Scan查询对象：HBase的查询入口，类似SQL的SELECT语句
            Scan scan = new Scan();
            // 3.1 读取需要的列族（基础信息+外部ID+评分信息+标签）
            scan.addFamily(Bytes.toBytes("basic"));       // 基本信息列族：title（标题）、genres（类型）
            scan.addFamily(Bytes.toBytes("external"));    // 外部ID列族：imdbId（IMDB平台ID）
            scan.addFamily(Bytes.toBytes("rating_stats"));// 评分信息列族：Flink计算后的avg_rating、total_ratings
            scan.addFamily(Bytes.toBytes("tags"));        // 标签信息列族：top_tag1（热门标签）

            // 3.2 构建过滤器：只筛选"basic:title"列包含关键词的行（精准过滤，减少无效数据传输）
            SingleColumnValueFilter titleFilter = new SingleColumnValueFilter(
                    Bytes.toBytes("basic"),          // 筛选的列族：只看basic列族
                    Bytes.toBytes("title"),          // 筛选的列名：只看title列（电影名）
                    CompareOperator.EQUAL,           // 匹配规则：EQUAL配合SubstringComparator实现"包含"逻辑
                    new SubstringComparator(title)   // 模糊匹配器：匹配包含title关键词的行
            );
            titleFilter.setFilterIfMissing(true);  // 过滤掉没有"basic:title"列的无效行（避免脏数据）
            scan.setFilter(titleFilter); // 将过滤器绑定到Scan对象

            // 4. 执行扫描查询：通过表对象获取结果扫描器
            scanner = table.getScanner(scan);
            // 5. 遍历结果集：逐条处理HBase返回的行数据（每行对应一部电影）
            for (Result result : scanner) {
                String movieId = Bytes.toString(result.getRow()); // 获取电影ID（rowkey）
                System.out.printf("\n===== 开始处理电影ID：%s =====\n", movieId); // 调试日志

                // 5.1 封装MovieAgg实体：将HBase字节数组数据转为Java对象
                MovieAgg movie = new MovieAgg();
                // 5.2 设置基础字段（保持原有逻辑不变）
                movie.setMovieId(movieId);
                movie.setTitle(Bytes.toString(result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("title"))));
                movie.setGenres(Bytes.toString(result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("genres"))));
                movie.setImdbId(Bytes.toString(result.getValue(Bytes.toBytes("external"), Bytes.toBytes("imdbId"))));
                movie.setTopTag1(Bytes.toString(result.getValue(Bytes.toBytes("tags"), Bytes.toBytes("top_tag1"))));

                // 【关键修改】从HBase读取Flink计算后的评分数据（替代内存缓存）
                String avgRatingStr = Bytes.toString(result.getValue(
                        Bytes.toBytes("rating_stats"), Bytes.toBytes("avg_rating")
                ));
                String totalRatingsStr = Bytes.toString(result.getValue(
                        Bytes.toBytes("rating_stats"), Bytes.toBytes("total_ratings")
                ));

                // 空值+格式校验（避免空指针和格式错误）
                double avgRating = (avgRatingStr != null && !avgRatingStr.isEmpty()) ? Double.parseDouble(avgRatingStr) : 0.0;
                int totalRatings = (totalRatingsStr != null && !totalRatingsStr.isEmpty()) ? Integer.parseInt(totalRatingsStr) : 0;

                // 设置评分字段（Flink计算后的最新结果）
                movie.setAvgRating(avgRating);
                movie.setTotalRatings(totalRatings);

                // 打印封装结果（调试用）
                System.out.printf("【封装结果】电影ID：%s | 标题：%s | Flink计算后评分：%s\n",
                        movie.getMovieId(), movie.getTitle(), movie.getAvgRating());

                // 添加到结果列表
                movieList.add(movie);
            }

        } catch (Exception e) {
            // 异常处理：打印堆栈信息，便于排查问题
            e.printStackTrace();
            System.err.println("【查询异常】电影查询失败：" + e.getMessage());
        } finally {
            // 安全关闭资源：无论查询是否报错，都必须关闭（避免HBase资源泄漏）
            if (scanner != null) try { scanner.close(); } catch (Exception e) {}
            if (table != null) try { table.close(); } catch (Exception e) {}
            if (hbaseConn != null) try { hbaseConn.close(); } catch (Exception e) {}
            System.out.println("\n===== 查询结束，资源已关闭 =====");
        }

        return movieList; // 返回结果列表：Spring自动转为JSON格式给前端
    }

    /**
     * 新增接口：接收前端发送的实时评分（核心逻辑不变，保留Kafka消息发送）
     * @param param 前端传递的参数（movieId：电影ID，rating：新评分）
     * @return 评分处理结果（成功/失败+更新后数据）
     */
    @PostMapping("/receive-rating")
    public Map<String, Object> receiveRealTimeRating(@RequestBody Map<String, Object> param) {
        try {
            // 1. 提取并校验参数
            if (param.get("movieId") == null || param.get("rating") == null) {
                Map<String, Object> errorMap = new HashMap<>();
                errorMap.put("success", false);
                errorMap.put("msg", "电影ID和评分不能为空");
                return errorMap;
            }

            String movieId = param.get("movieId").toString();
            double newRating = Double.parseDouble(param.get("rating").toString());

            // 校验评分范围（1-5分）
            if (newRating < 1.0 || newRating > 5.0) {
                Map<String, Object> errorMap = new HashMap<>();
                errorMap.put("success", false);
                errorMap.put("msg", "评分必须在1-5分之间");
                return errorMap;
            }

            // 2. 构建Kafka消息实体（封装电影ID、新评分、时间戳）
            // 修改后：构建 KafkaRatingMessage 对象（和 Producer 接收的类型一致）
            KafkaRatingMessage ratingMsg = new KafkaRatingMessage();
            ratingMsg.setMovieId(movieId);
            ratingMsg.setRating(newRating);
            ratingMsg.setTimestamp(System.currentTimeMillis());

            // 调用 KafkaRatingProducer 的 sendRatingMessage 方法（传 KafkaRatingMessage）
            kafkaRatingProducer.sendRatingMessage(ratingMsg); // 关键：用 Producer 工具类发送，不是直接用 kafkaTemplate
            System.out.printf("【Kafka消息发送成功】主题：%s | 电影ID：%s | 评分：%s\n",
                    KAFKA_RATING_TOPIC, movieId, newRating);

            // 4. 推送更新到所有在线前端（WebSocket实时刷新）
            Map<String, Object> pushData = new HashMap<>();
            pushData.put("movieId", movieId);
            pushData.put("currentRating", newRating);
            WebSocketServer.pushRatingUpdate(pushData);

            // 5. 返回成功结果给前端
            Map<String, Object> successMap = new HashMap<>();
            successMap.put("success", true);
            successMap.put("msg", "评分提交成功（已发送到Kafka，等待Flink计算）");
            successMap.put("data", pushData);
            return successMap;

        } catch (Exception e) {
            // 异常处理：返回失败信息（包含具体错误原因）
            Map<String, Object> errorMap = new HashMap<>();
            errorMap.put("success", false);
            errorMap.put("msg", "评分提交失败：" + e.getMessage());
            System.err.println("【评分提交异常】" + e.getMessage());
            return errorMap;
        }
    }

    /**
     * 工具方法：打印Result中的所有列数据（调试用，保留不变）
     * 作用：排查"字段不存在"问题（如确认rating_stats列族是否被Flink正确写入）
     * @param result HBase查询返回的单行结果
     * @return 是否读取到rating_stats列族
     */
    private boolean printAllColumns(Result result) {
        boolean hasRatingStats = false;
        for (Cell cell : result.rawCells()) {
            String family = Bytes.toString(CellUtil.cloneFamily(cell));
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            System.out.printf("【列数据】列族：%s | 列名：%s | 值：%s\n", family, qualifier, value);
            if ("rating_stats".equals(family)) {
                hasRatingStats = true;
            }
        }
        System.out.printf("【关键标记】是否读取到rating_stats列族：%s\n", hasRatingStats ? "是" : "否");
        return hasRatingStats;
    }

    /**
     * 工具方法：获取Result中指定列族+列名的值（字符串类型，保留不变）
     * @param result HBase单行结果
     * @param family 列族名
     * @param qualifier 列名
     * @return 列值（若为null返回空字符串）
     */
    private String getColumnValue(Result result, String family, String qualifier) {
        byte[] valueBytes = result.getValue(
                Bytes.toBytes(family),
                Bytes.toBytes(qualifier)
        );
        return valueBytes != null ? Bytes.toString(valueBytes) : "";
    }
}