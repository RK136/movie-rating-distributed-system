package com.movielens.service; // 包声明：服务层，负责封装业务逻辑（介于控制器和数据库之间）

// 导入依赖类：实体类、HBase工具类、HBase核心API、Spring注解等
import com.movielens.entity.MovieAgg; // 电影汇总信息实体类（存储查询结果）
import com.movielens.utils.HBaseClientUtil; // HBase连接工具类（单例模式管理连接）
import org.apache.hadoop.hbase.TableName; // HBase表名封装类
import org.apache.hadoop.hbase.client.Connection; // HBase连接接口
import org.apache.hadoop.hbase.client.Get; // HBase的Get查询对象（按行键精确查询）
import org.apache.hadoop.hbase.client.Table; // HBase表操作对象
import org.apache.hadoop.hbase.util.Bytes; // HBase字节数组工具类（字节与字符串/数值互转）
import org.springframework.stereotype.Service; // Spring服务层注解（标识为业务逻辑组件）

import java.io.IOException; // IO异常（HBase操作可能抛出）


/**
 * MovieService类 - 电影服务实现类
 * 作用：封装电影相关的业务逻辑（如查询电影详情、处理评分数据等），
 * 隔离控制器（Controller）和数据访问层（HBase操作），使代码分层更清晰
 */
@Service // Spring注解：标记为服务层组件，由Spring自动管理（可被控制器注入使用）
public class MovieService {

    /**
     * 根据电影ID（movieId）查询电影详情（包含平均评分等核心信息）
     * 业务场景：适用于"通过电影唯一ID查询详细信息"的需求（如点击电影名称查看详情）
     * @param movieId 电影唯一标识（对应HBase表的行键rowkey）
     * @return 封装好的电影详情对象（MovieAgg），包含标题、类型、平均评分等
     * @throws IOException HBase连接或查询异常（如连接失败、表不存在）
     */
    public MovieAgg getMovieById(String movieId) throws IOException {
        // 1. 获取HBase连接：复用工具类的单例连接（全局唯一，避免频繁创建销毁）
        // 设计亮点：HBase连接创建开销大（需与ZK通信协商集群信息），单例模式可显著提升性能
        Connection conn = HBaseClientUtil.getConnection();
        // 1.1 获取HBase表对象：通过连接获取"movie_aggregate"表的操作句柄（类似数据库的表连接）
        Table table = conn.getTable(TableName.valueOf("movie_aggregate"));

        // 2. 构造Get查询对象：HBase中按行键（rowkey）精确查询的核心对象（类似SQL的WHERE id=?）
        // 行键为movieId（如"1"、"2"），确保查询的是指定电影的行数据
        Get get = new Get(Bytes.toBytes(movieId)); // Bytes.toBytes：字符串转字节数组（HBase底层存储格式）

        // 2.1 显式指定要读取的列：精确控制需要的字段，减少无效数据传输（提升查询效率）
        // 读取评分列族的平均评分字段（核心字段：rating_stats:avg_rating）
        get.addColumn(Bytes.toBytes("rating_stats"), Bytes.toBytes("avg_rating"));
        // 读取基本信息列族的标题和类型字段（补充必要信息）
        get.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("title"));
        get.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("genres"));
        // 说明：若需要其他字段（如imdbId、标签），可继续添加addColumn(列族, 列名)

        // 3. 执行查询：通过表对象的get方法，获取指定行键的结果（Result包含该行的所有查询列数据）
        org.apache.hadoop.hbase.client.Result result = table.get(get);

        // 4. 手动映射数据到MovieAgg实体：将HBase的字节数组结果转为Java对象（便于前端使用）
        MovieAgg movieAgg = new MovieAgg(); // 创建实体对象
        movieAgg.setMovieId(movieId); // 设置电影ID（直接复用入参，无需从Result获取）

        // 4.1 映射标题字段（basic:title）：先判断列是否存在，避免空指针
        // containsColumn：检查Result中是否包含指定列族+列名的数据（防止数据缺失导致的异常）
        if (result.containsColumn(Bytes.toBytes("basic"), Bytes.toBytes("title"))) {
            // 提取列值：从Result中获取字节数组，转为字符串（标题是字符串类型）
            String title = Bytes.toString(result.getValue(
                    Bytes.toBytes("basic"),   // 列族：basic
                    Bytes.toBytes("title")    // 列名：title
            ));
            movieAgg.setTitle(title); // 赋值给实体对象
        }

        // 4.2 映射平均评分字段（rating_stats:avg_rating）：核心逻辑，处理数值类型
        // 先判断列是否存在（避免某部电影没有评分数据时的空指针）
        if (result.containsColumn(Bytes.toBytes("rating_stats"), Bytes.toBytes("avg_rating"))) {
            // 提取列值并转换：从字节数组转为Double（与MovieAgg的avgRating字段类型匹配）
            // 注意：此处使用Bytes.toDouble()的前提是HBase中该字段存储的是Double类型的字节数组
            // 若HBase中存储的是字符串（如"3.9"），直接使用toDouble()会报"数组容量不足"错误（需先转字符串）
            Double avgRating = Bytes.toDouble(result.getValue(
                    Bytes.toBytes("rating_stats"), // 列族：rating_stats
                    Bytes.toBytes("avg_rating")    // 列名：avg_rating
            ));
            movieAgg.setAvgRating(avgRating); // 赋值真实评分（如3.9、4.5）
            // 调试日志：打印评分值，验证是否正确读取（便于排查"评分始终为0.0"的问题）
            System.out.println("调试：平均评分=" + avgRating);
        }

        // 4.3 （可选）映射类型字段（basic:genres）：逻辑同标题字段
        if (result.containsColumn(Bytes.toBytes("basic"), Bytes.toBytes("genres"))) {
            String genres = Bytes.toString(result.getValue(
                    Bytes.toBytes("basic"),
                    Bytes.toBytes("genres")
            ));
            movieAgg.setGenres(genres);
        }

        // 5. 关闭资源：只关闭表对象（HBase连接是单例，由工具类管理，不在这里关闭）
        // 设计亮点：及时关闭Table避免资源泄漏（Table是稀缺资源，不关闭会导致HBase连接池耗尽）
        table.close();

        // 6. 返回封装好的实体对象：供控制器（Controller）返回给前端
        return movieAgg;
    }
}