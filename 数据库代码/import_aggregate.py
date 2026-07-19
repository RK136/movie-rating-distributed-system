import happybase
from collections import defaultdict
import time

# 连接 HBase（本地操作保持 localhost；多人协作时替换为 A 的局域网 IP，如 192.168.1.100）
hbase_host = 'localhost'
connection = happybase.Connection(host=hbase_host, port=9090, autoconnect=True)

# 获取原始表对象
movies_table = connection.table('movies')
links_table = connection.table('links')
ratings_table = connection.table('ratings')
tags_table = connection.table('tags')

# 汇总表对象（需先在 HBase Shell 创建）
aggregate_table = connection.table('movie_aggregate')

# 批量写入参数（根据数据量调整，数据量大可设为 1000）
batch_size = 500
batch = aggregate_table.batch(batch_size=batch_size)

# 临时存储评分和标签的聚合结果
rating_aggregates = defaultdict(lambda: {'ratings': [], 'timestamps': []})  # {movieId: {评分列表, 时间戳列表}}
tag_aggregates = defaultdict(lambda: defaultdict(int))  # {movieId: {tag: 出现次数}}


def process_ratings():
    """处理 ratings 表，计算每个 movieId 的评分统计（平均值、总次数、最新评分）"""
    print("开始处理 ratings 表...")
    count = 0
    # 扫描所有评分数据（rowkey 格式：userId_movieId）
    for rowkey, data in ratings_table.scan():
        # 解析 rowkey 得到 movieId（例如 rowkey=1_100 → movieId=100）
        movie_id = rowkey.decode('utf-8').split('_')[1]
        # 提取评分（转换为浮点数）和时间戳（转换为整数）
        rating = float(data[b'rating_info:rating'].decode('utf-8'))
        timestamp = int(data[b'rating_info:timestamp'].decode('utf-8'))
        # 存入临时字典
        rating_aggregates[movie_id]['ratings'].append(rating)
        rating_aggregates[movie_id]['timestamps'].append(timestamp)
        # 每处理 10000 条打印一次进度
        count += 1
        if count % 10000 == 0:
            print(f"已处理 {count} 条评分数据")


def process_tags():
    """处理 tags 表，统计每个 movieId 的热门标签（出现次数前 2 的标签）"""
    print("开始处理 tags 表...")
    count = 0
    # 扫描所有标签数据（rowkey 格式：userId_movieId）
    for rowkey, data in tags_table.scan():
        movie_id = rowkey.decode('utf-8').split('_')[1]
        tag = data[b'tag_info:tag'].decode('utf-8')
        # 统计标签出现次数（相同标签累加）
        tag_aggregates[movie_id][tag] += 1
        # 每处理 1000 条打印一次进度
        count += 1
        if count % 1000 == 0:
            print(f"已处理 {count} 条标签数据")


def build_aggregate_table():
    """整合所有数据，写入汇总表"""
    print("开始构建汇总表...")
    total_movies = 0
    # 扫描 movies 表，以 movieId 为基准整合数据
    for movie_id_bytes, movie_data in movies_table.scan():
        movie_id = movie_id_bytes.decode('utf-8')  # 转换为字符串格式
        total_movies += 1
        
        # 1. 整合 basic 列族（电影基本信息）
        basic_data = {
            'basic:title': movie_data[b'basic:title'].decode('utf-8'),
            'basic:genres': movie_data[b'basic:genres'].decode('utf-8')
        }
        
        # 2. 整合 external 列族（外部 ID）
        external_data = {}
        # 从 links 表查询当前 movieId 的数据
        links_data = links_table.row(movie_id_bytes)
        if links_data:  # 若存在数据则添加
            external_data = {
                'external:imdbId': links_data[b'external:imdbId'].decode('utf-8'),
                'external:tmdbId': links_data[b'external:tmdbId'].decode('utf-8')
            }
        
        # 3. 整合 rating_stats 列族（评分统计）
        rating_stats = {}
        if movie_id in rating_aggregates:
            ratings = rating_aggregates[movie_id]['ratings']
            timestamps = rating_aggregates[movie_id]['timestamps']
            avg_rating = round(sum(ratings) / len(ratings), 1)  # 平均评分（保留1位小数）
            total_ratings = len(ratings)  # 总评分次数
            # 最新评分（取时间戳最大的评分）
            latest_idx = timestamps.index(max(timestamps))
            latest_rating = ratings[latest_idx]
            rating_stats = {
                'rating_stats:avg_rating': str(avg_rating),
                'rating_stats:total_ratings': str(total_ratings),
                'rating_stats:latest_rating': str(latest_rating)
            }
        
        # 4. 整合 tags 列族（热门标签）
        tag_data = {}
        if movie_id in tag_aggregates:
            # 按标签出现次数降序排序，取前2名
            sorted_tags = sorted(tag_aggregates[movie_id].items(), key=lambda x: x[1], reverse=True)
            tag_count = sum(tag_aggregates[movie_id].values())  # 标签总数量
            tag_data['tags:tag_count'] = str(tag_count)
            # 取前2个热门标签（不足2个则只存有的）
            for i in range(min(2, len(sorted_tags))):
                tag_data[f'tags:top_tag{i+1}'] = sorted_tags[i][0]
        
        # 合并所有数据，写入汇总表（rowkey 为 movieId）
        all_data = {**basic_data,** external_data, **rating_stats,** tag_data}
        batch.put(movie_id, all_data)
        
        # 每处理 1000 部电影打印一次进度
        if total_movies % 1000 == 0:
            print(f"已写入 {total_movies} 条汇总数据")
    
    # 提交最后一批未达 batch_size 的数据
    batch.send()
    print(f"汇总表构建完成！共处理 {total_movies} 部电影")


if __name__ == '__main__':
    try:
        process_ratings()    # 第一步：处理评分数据
        process_tags()       # 第二步：处理标签数据
        build_aggregate_table()  # 第三步：整合并写入汇总表
    except Exception as e:
        print(f"执行失败：{str(e)}")
    finally:
        connection.close()  # 确保连接关闭
