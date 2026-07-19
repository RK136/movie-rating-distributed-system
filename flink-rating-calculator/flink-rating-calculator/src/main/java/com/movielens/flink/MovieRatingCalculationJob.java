package com.movielens.flink;

import com.google.gson.Gson;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration; // Flink的Configuration
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Flink 实时评分计算（终极稳定累计版）
 * 核心特性：1. 评分永久累计（无窗口，持续累加）；2. 数据永久存入HBase；3. 重启不丢失；4. 前端实时展示
 * 输入：Kafka(movie-rating-topic) → 计算：累计平均分+总次数+最高分 → 输出：HBase（永久存储）+ Kafka（实时推送）
 */
public class MovieRatingCalculationJob {

    // 环境配置（匹配你的集群，无需修改）
    private static final String KAFKA_BOOTSTRAP = "192.168.56.101:9092";
    private static final String HBASE_ZK_QUORUM = "192.168.56.101";
    private static final String HBASE_ZK_PORT = "2182";
    private static final String HBASE_TABLE_NAME = "movie_aggregate";
    private static final String HBASE_COLUMN_FAMILY = "rating_stats";
    private static final Gson GSON = new Gson();

    // HBase 全局连接（复用，避免泄漏）
    private static Connection hbaseConn;
    private static Table hbaseTable;

    // 静态初始化 HBase 连接（程序启动时执行，确保连接就绪）
    static {
        try {
            // 直接使用 Hadoop Configuration 全类名，彻底避免重名冲突
            org.apache.hadoop.conf.Configuration hbaseConf = HBaseConfiguration.create();
            hbaseConf.set("hbase.zookeeper.quorum", HBASE_ZK_QUORUM);
            hbaseConf.set("hbase.zookeeper.property.clientPort", HBASE_ZK_PORT);
            hbaseConf.set("hbase.client.retries.number", "3");
            hbaseConf.set("hbase.rpc.timeout", "3000");
            hbaseConf.set("hbase.client.operation.timeout", "5000");
            hbaseConf.set("hbase.client.ipc.pool.size", "5");

            hbaseConn = ConnectionFactory.createConnection(hbaseConf);
            hbaseTable = hbaseConn.getTable(TableName.valueOf(HBASE_TABLE_NAME));
            System.out.println("[HBase初始化] 连接成功，表已打开（全局复用）");
        } catch (Exception e) {
            System.err.println("[HBase初始化] 连接失败：" + e.getMessage());
            e.printStackTrace();
            System.exit(1); // 初始化失败直接退出，避免后续报错
        }
    }

    // 新增：从HBase读取电影历史累计数据（供Flink状态初始化）
    private static Tuple3<Double, Integer, Double> getHBaseHistory(String movieId) {
        try {
            if (hbaseTable == null) {
                System.err.println("[HBase历史读取] 表对象未初始化，返回空");
                return null;
            }

            // 构造HBase查询条件（行键=movieId）
            Get get = new Get(Bytes.toBytes(movieId));
            // 只查询需要的列（避免多余数据）
            get.addColumn(Bytes.toBytes(HBASE_COLUMN_FAMILY), Bytes.toBytes("avg_rating"));
            get.addColumn(Bytes.toBytes(HBASE_COLUMN_FAMILY), Bytes.toBytes("total_ratings"));
            get.addColumn(Bytes.toBytes(HBASE_COLUMN_FAMILY), Bytes.toBytes("max_rating"));

            Result result = hbaseTable.get(get);
            if (result.isEmpty()) {
                System.out.printf("[HBase历史读取] movieId=%s 无历史数据\n", movieId);
                return null;
            }

            // 读取HBase中的历史数据
            String avgRatingStr = Bytes.toString(result.getValue(
                    Bytes.toBytes(HBASE_COLUMN_FAMILY), Bytes.toBytes("avg_rating")
            ));
            String totalRatingsStr = Bytes.toString(result.getValue(
                    Bytes.toBytes(HBASE_COLUMN_FAMILY), Bytes.toBytes("total_ratings")
            ));
            String maxRatingStr = Bytes.toString(result.getValue(
                    Bytes.toBytes(HBASE_COLUMN_FAMILY), Bytes.toBytes("max_rating")
            ));

            // 校验数据有效性（避免格式错误）
            if (avgRatingStr == null || totalRatingsStr == null || maxRatingStr == null) {
                System.err.printf("[HBase历史读取] movieId=%s 数据不完整\n", movieId);
                return null;
            }

            double avgRating = Double.parseDouble(avgRatingStr);
            int totalRatings = Integer.parseInt(totalRatingsStr);
            double maxRating = Double.parseDouble(maxRatingStr);
            double totalScore = avgRating * totalRatings; // 计算历史总分（用于后续累加）

            System.out.printf("[HBase历史读取] 成功：movieId=%s，历史总分=%.1f，历史次数=%d，历史最高分=%.1f\n",
                    movieId, totalScore, totalRatings, maxRating);

            // 返回 Tuple3（总分、次数、最高分），与Flink状态格式一致
            return Tuple3.of(totalScore, totalRatings, maxRating);
        } catch (Exception e) {
            System.err.printf("[HBase历史读取] 失败：movieId=%s，错误：%s\n", movieId, e.getMessage());
            return null;
        }
    }

    public static void main(String[] args) throws Exception {
        // 1. 创建Flink流处理环境（启用Checkpoint，增强稳定性）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000); // 每5秒Checkpoint

        // 2. Kafka消费者配置（速率限制，避免数据积压）
        Properties kafkaConsumerProps = new Properties();
        kafkaConsumerProps.setProperty("bootstrap.servers", KAFKA_BOOTSTRAP);
        kafkaConsumerProps.setProperty("group.id", "flink-movie-rating-group");
        kafkaConsumerProps.setProperty("auto.offset.reset", "earliest");
        kafkaConsumerProps.setProperty("max.poll.records", "5");
        kafkaConsumerProps.setProperty("fetch.max.bytes", "1048576");

        // 3. 读取Kafka数据（Gson解析+严格校验）
        DataStream<Map<String, Object>> ratingStream = env.addSource(
                        new FlinkKafkaConsumer<>("movie-rating-topic", new SimpleStringSchema(), kafkaConsumerProps)
                ).map(new MapFunction<String, Map<String, Object>>() {
                    @Override
                    public Map<String, Object> map(String jsonStr) throws Exception {
                        System.out.printf("[Flink输入] 收到Kafka原始数据：%s\n", jsonStr);
                        try {
                            Map<String, Object> map = GSON.fromJson(jsonStr, Map.class);
                            // 校验必填字段和数据类型
                            if (map == null || !map.containsKey("movieId") || !map.containsKey("rating")) {
                                System.err.printf("[Flink输入] 数据缺少字段：%s\n", jsonStr);
                                return null;
                            }
                            Object ratingObj = map.get("rating");
                            if (!(ratingObj instanceof Number)) {
                                System.err.printf("[Flink输入] rating不是数字：%s\n", jsonStr);
                                return null;
                            }
                            return map;
                        } catch (Exception e) {
                            System.err.printf("[Flink输入] 解析失败：%s，错误：%s\n", jsonStr, e.getMessage());
                            return null;
                        }
                    }
                }).filter(map -> map != null)
                .name("Kafka-Source-Map");

        // 4. 核心：全局累计聚合（无窗口，永久累加）
        DataStream<Tuple4<String, Double, Integer, Double>> resultStream = ratingStream
                .keyBy(map -> map.get("movieId").toString()) // 按movieId分组
                .process(new KeyedProcessFunction<String, Map<String, Object>, Tuple4<String, Double, Integer, Double>>() {
                    private ValueState<Tuple3<Double, Integer, Double>> ratingState; // 累计状态（总分、次数、最高分）

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 初始化Flink状态（作业运行期间不丢失）
                        ValueStateDescriptor<Tuple3<Double, Integer, Double>> stateDesc = new ValueStateDescriptor<>(
                                "permanentRatingState",
                                TypeInformation.of(new TypeHint<Tuple3<Double, Integer, Double>>() {})
                        );
                        ratingState = getRuntimeContext().getState(stateDesc);
                    }

                    @Override
                    public void processElement(Map<String, Object> map, Context ctx, Collector<Tuple4<String, Double, Integer, Double>> out) throws Exception {
                        String movieId = map.get("movieId").toString();
                        double newRating = ((Number) map.get("rating")).doubleValue();

                        // 读取已有累计状态（首次为null）
                        Tuple3<Double, Integer, Double> currentState = ratingState.value();
                        if (currentState == null) {
                            // 新增：从HBase读取历史数据初始化状态（重启后关键）
                            Tuple3<Double, Integer, Double> hbaseHistory = getHBaseHistory(movieId);
                            if (hbaseHistory != null) {
                                // 有历史数据：用HBase数据初始化（总分、次数、最高分）
                                currentState = hbaseHistory;
                            } else {
                                // 无历史数据：首次初始化（新电影）
                                currentState = Tuple3.of(newRating, 1, newRating);
                            }
                        } else {
                            // 累计更新：总分+新评分，次数+1，最高分取最大值
                            double totalScore = currentState.f0 + newRating;
                            int totalCount = currentState.f1 + 1;
                            double maxScore = Math.max(currentState.f2, newRating);
                            currentState = Tuple3.of(totalScore, totalCount, maxScore);
                        }

                        // 更新状态并计算平均分
                        ratingState.update(currentState);
                        double avgScore = Math.round(currentState.f0 / currentState.f1 * 10) / 10.0;

                        // 输出累计结果（供HBase写入和Kafka推送）
                        System.out.printf("[Flink计算] 累计结果：movieId=%s，平均分=%.1f，总次数=%d，最高分=%.1f\n",
                                movieId, avgScore, currentState.f1, currentState.f2);
                        out.collect(Tuple4.of(movieId, avgScore, currentState.f1, currentState.f2));
                    }
                }).name("Permanent-Rating-Aggregate");

        // 5. 写入HBase（永久存储，重启不丢失）
        resultStream.addSink(new SinkFunction<Tuple4<String, Double, Integer, Double>>() {
            @Override
            public void invoke(Tuple4<String, Double, Integer, Double> t, Context context) throws Exception {
                if (hbaseTable == null) {
                    System.err.println("[HBase输出] 表对象未初始化，跳过写入");
                    return;
                }

                try {
                    // 行键=movieId，列族=rating_stats，存储累计数据
                    Put put = new Put(Bytes.toBytes(t.f0));
                    put.addColumn(
                            Bytes.toBytes(HBASE_COLUMN_FAMILY),
                            Bytes.toBytes("avg_rating"),
                            Bytes.toBytes(String.valueOf(t.f1))
                    );
                    put.addColumn(
                            Bytes.toBytes(HBASE_COLUMN_FAMILY),
                            Bytes.toBytes("total_ratings"),
                            Bytes.toBytes(String.valueOf(t.f2))
                    );
                    put.addColumn(
                            Bytes.toBytes(HBASE_COLUMN_FAMILY),
                            Bytes.toBytes("max_rating"),
                            Bytes.toBytes(String.valueOf(t.f3))
                    );
                    hbaseTable.put(put);
                    System.out.printf("[HBase输出] 永久存储成功：movieId=%s，平均分=%.1f，总次数=%d，最高分=%.1f\n",
                            t.f0, t.f1, t.f2, t.f3);
                } catch (Exception e) {
                    System.err.printf("[HBase输出] 写入失败：movieId=%s，错误：%s\n", t.f0, e.getMessage());
                    // 连接断开时尝试重新初始化
                    if (e.getMessage() != null && (e.getMessage().contains("closed") || e.getMessage().contains("Connection"))) {
                        reinitHBaseConnection();
                    }
                }
            }

            // HBase连接重连（静态方法，可直接调用）
            private void reinitHBaseConnection() {
                try {
                    if (hbaseConn != null) {
                        hbaseConn.close();
                    }
                    // 全类名指定Hadoop Configuration，避免冲突
                    org.apache.hadoop.conf.Configuration hbaseConf = HBaseConfiguration.create();
                    hbaseConf.set("hbase.zookeeper.quorum", HBASE_ZK_QUORUM);
                    hbaseConf.set("hbase.zookeeper.property.clientPort", HBASE_ZK_PORT);
                    hbaseConf.set("hbase.client.retries.number", "3");
                    hbaseConf.set("hbase.rpc.timeout", "3000");
                    hbaseConn = ConnectionFactory.createConnection(hbaseConf);
                    hbaseTable = hbaseConn.getTable(TableName.valueOf(HBASE_TABLE_NAME));
                    System.out.println("[HBase重新初始化] 连接成功");
                } catch (Exception e) {
                    System.err.println("[HBase重新初始化] 失败：" + e.getMessage());
                    hbaseTable = null;
                    hbaseConn = null;
                }
            }

            // 自定义关闭方法（释放HBase资源）
            public void close() throws Exception {
                System.out.println("[HBase连接] 开始关闭资源...");
                if (hbaseTable != null) {
                    try {
                        hbaseTable.close();
                        System.out.println("[HBase连接] 表已关闭");
                    } catch (Exception e) {
                        System.err.println("[HBase连接] 表关闭失败：" + e.getMessage());
                    }
                }
                if (hbaseConn != null) {
                    try {
                        hbaseConn.close();
                        System.out.println("[HBase连接] 连接已关闭");
                    } catch (Exception e) {
                        System.err.println("[HBase连接] 连接关闭失败：" + e.getMessage());
                    }
                }
            }
        }).name("HBase-Permanent-Writer");

        // 6. 写入Kafka（实时推送累计结果，供前端展示）
        DataStream<Map<String, Object>> kafkaOutputMap = resultStream.map(new MapFunction<Tuple4<String, Double, Integer, Double>, Map<String, Object>>() {
            @Override
            public Map<String, Object> map(Tuple4<String, Double, Integer, Double> t) throws Exception {
                if (t == null || t.f0 == null || t.f0.isEmpty() || t.f2 == 0) {
                    System.err.printf("[Kafka输出] 数据无效：%s\n", t);
                    return null;
                }
                Map<String, Object> resultMap = new HashMap<>();
                resultMap.put("movieId", t.f0);
                resultMap.put("avgRating", t.f1);
                resultMap.put("totalRatings", t.f2);
                resultMap.put("maxRating", t.f3);
                return resultMap;
            }
        }).filter(map -> map != null).name("Result-To-Map");

        // Kafka生产者配置
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", KAFKA_BOOTSTRAP);
        kafkaProps.setProperty("acks", "1");
        kafkaProps.setProperty("retries", "3");
        kafkaProps.setProperty("compression.type", "snappy");
        kafkaProps.setProperty("batch.size", "16384");
        kafkaProps.setProperty("linger.ms", "5");
        kafkaProps.setProperty("buffer.memory", "33554432");

        // Kafka序列化（显式转换为JSON字符串，无冲突）
        FlinkKafkaProducer<Map<String, Object>> kafkaProducer = new FlinkKafkaProducer<>(
                "movie-rating-update-topic",
                new KafkaSerializationSchema<Map<String, Object>>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(Map<String, Object> map, @Nullable Long timestamp) {
                        if (map == null) {
                            return null;
                        }
                        String jsonStr = GSON.toJson(map);
                        System.out.printf("[Kafka输出] 推送累计数据：%s\n", jsonStr);
                        return new ProducerRecord<>(
                                "movie-rating-update-topic",
                                jsonStr.getBytes()
                        );
                    }
                },
                kafkaProps,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
        );

        // 添加Kafka Sink并启动作业
        kafkaOutputMap.addSink(kafkaProducer).name("Kafka-RealTime-Writer").setParallelism(1);
        env.execute("Movie-Rating-Permanent-Calc");
    }
}