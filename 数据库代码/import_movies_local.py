import happybase
import csv

# 连接 HBase（Thrift 服务默认端口 9090）
connection = happybase.Connection(host='localhost', port=9090, autoconnect=True)
table = connection.table('movies')  # 目标表名
print("已连接到 movies 表")

# 本地 CSV 路径（替换为你的实际路径）
local_csv = "/home/hadoop/processed_moviedata/movies.csv"
batch = table.batch(batch_size=100)  # 批量写入

try:
    with open(local_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)  # 按列名读取
        count = 0
        for row in reader:
            rowkey = row['movieId']  # 行键为 movieId
            data = {
                'basic:title': row['title'],
                'basic:genres': row['genres']
            }
            batch.put(rowkey, data)
            count += 1
            if count % 1000 == 0:
                print(f"movies 已导入 {count} 条")
        batch.send()  # 提交剩余数据
        print(f"movies 导入完成，共 {count} 条")
except Exception as e:
    print(f"movies 导入失败：{e}")
finally:
    connection.close()
