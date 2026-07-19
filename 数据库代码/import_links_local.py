import happybase
import csv

connection = happybase.Connection('localhost', 9090, autoconnect=True)
table = connection.table('links')
print("已连接到 links 表")

local_csv = "/home/hadoop/processed_moviedata/links.csv"
batch = table.batch(batch_size=100)

try:
    with open(local_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        count = 0
        for row in reader:
            rowkey = row['movieId']
            data = {
                'external:imdbId': row['imdbId'],
                'external:tmdbId': row['tmdbId']
            }
            batch.put(rowkey, data)
            count += 1
            if count % 1000 == 0:
                print(f"links 已导入 {count} 条")
        batch.send()
        print(f"links 导入完成，共 {count} 条")
except Exception as e:
    print(f"links 导入失败：{e}")
finally:
    connection.close()
