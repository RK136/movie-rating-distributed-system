import happybase
import csv

connection = happybase.Connection('localhost', 9090, autoconnect=True)
table = connection.table('tags')
print("已连接到 tags 表")

local_csv = "/home/hadoop/processed_moviedata/tags.csv"
batch = table.batch(batch_size=100)

try:
    with open(local_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        count = 0
        for row in reader:
            rowkey = row['rowkey']  # 行键为预处理生成的 userId_movieId
            data = {
                'tag_info:tag': row['tag'],
                'tag_info:timestamp': row['timestamp']
            }
            batch.put(rowkey, data)
            count += 1
            if count % 1000 == 0:
                print(f"tags 已导入 {count} 条")
        batch.send()
        print(f"tags 导入完成，共 {count} 条")
except Exception as e:
    print(f"tags 导入失败：{e}")
finally:
    connection.close()
