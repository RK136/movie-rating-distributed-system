import pandas as pd
import re
import os

# 源CSV文件所在目录（你的路径，无需修改）
data_dir = "/home/hadoop/moviedata"
# 预处理后文件的保存目录（自动创建）
processed_dir = "/home/hadoop/processed_moviedata"
os.makedirs(processed_dir, exist_ok=True)  # 确保目录存在

# 清洗函数：移除特殊字符，标准化格式
def clean_value(value):
    if isinstance(value, str):
        # 移除换行符、制表符，空格替换为下划线（避免HBase列名/值冲突）
        return re.sub(r'[\n\t]', '', value).replace(' ', '_')
    return str(value)  # 非字符串类型转为字符串（如数字、时间戳）

try:
    # 1. 处理 movies.csv
    movies_path = os.path.join(data_dir, "movies.csv")
    movies_df = pd.read_csv(movies_path)
    movies_df = movies_df.fillna("未知")  # 缺失值填充
    for col in movies_df.columns:
        movies_df[col] = movies_df[col].apply(clean_value)
    movies_df.to_csv(os.path.join(processed_dir, "movies.csv"), index=False)
    print(f"✅ 成功处理：movies.csv（保存至 {processed_dir}）")

    # 2. 处理 links.csv
    links_path = os.path.join(data_dir, "links.csv")
    links_df = pd.read_csv(links_path)
    links_df = links_df.fillna("未知")
    for col in links_df.columns:
        links_df[col] = links_df.apply(lambda row: clean_value(row[col]), axis=1)
    links_df.to_csv(os.path.join(processed_dir, "links.csv"), index=False)
    print(f"✅ 成功处理：links.csv")

    # 3. 处理 ratings.csv（生成复合行键 userId_movieId）
    ratings_path = os.path.join(data_dir, "ratings.csv")
    ratings_df = pd.read_csv(ratings_path)
    ratings_df = ratings_df.fillna("未知")
    # 合并 userId 和 movieId 作为行键（放在第一列，供HBase导入时使用）
    ratings_df['rowkey'] = ratings_df['userId'].astype(str) + '_' + ratings_df['movieId'].astype(str)
    # 保留行键、评分、时间戳（删除原始userId和movieId，避免重复）
    ratings_df = ratings_df[['rowkey', 'rating', 'timestamp']]
    for col in ratings_df.columns:
        ratings_df[col] = ratings_df[col].apply(clean_value)
    ratings_df.to_csv(os.path.join(processed_dir, "ratings.csv"), index=False)
    print(f"✅ 成功处理：ratings.csv（已生成复合行键）")

    # 4. 处理 tags.csv（生成复合行键 userId_movieId）
    tags_path = os.path.join(data_dir, "tags.csv")
    tags_df = pd.read_csv(tags_path)
    tags_df = tags_df.fillna("未知")
    # 合并 userId 和 movieId 作为行键
    tags_df['rowkey'] = tags_df['userId'].astype(str) + '_' + tags_df['movieId'].astype(str)
    # 保留行键、标签、时间戳
    tags_df = tags_df[['rowkey', 'tag', 'timestamp']]
    for col in tags_df.columns:
    	tags_df[col] = tags_df[col].apply(clean_value)
    tags_df.to_csv(os.path.join(processed_dir, "tags.csv"), index=False)
    print(f"✅ 成功处理：tags.csv（已生成复合行键）")

except FileNotFoundError as e:
    print(f"❌ 错误：找不到文件 - {e}")
except Exception as e:
    print(f"❌ 处理失败：{e}")
