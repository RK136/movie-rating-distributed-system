import happybase
import csv

connection = happybase.Connection('localhost', 9090, autoconnect=True)
table = connection.table('ratings')
print("已连接到 ratings 表")

local_csv = "/home/hadoop/processed_moviedata/ratings.csv"
batch = table.batch(batch_size=100)

try:
    with open(local_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        count = 0
        for row in reader:
            rowkey = row['rowkey']  # 行键为预处理生成的 userId_movieId
            data = {
                'rating_info:rating': row['rating'],
                'rating_info:timestamp': row['timestamp']
            }
            batch.put(rowkey, data)
            count += 1
            if count % 1000 == 0:
                print(f"ratings 已导入 {count} 条")
        batch.send()
        print(f"ratings 导入完成，共 {count} 条")
except Exception as e:
    print(f"ratings 导入失败：{e}")
finally:
    connection.close()
