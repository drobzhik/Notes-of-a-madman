from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import DuplicateKeyError
from bson.objectid import ObjectId

# Подключение
client = MongoClient('mongodb://localhost:27017/')
db = client['mydatabase']
collection = db['mycollection']

# ===== CREATE =====
# Вставка одного документа
doc = {"name": "John", "age": 30, "city": "New York"}
insert_one_result = collection.insert_one(doc)
print(f"Inserted ID: {insert_one_result.inserted_id}")

# Вставка нескольких документов
docs = [
    {"name": "Alice", "age": 25, "city": "Chicago"},
    {"name": "Bob", "age": 35, "city": "Boston"},
    {"name": "Charlie", "age": 40, "city": "Seattle"}
]
insert_many_result = collection.insert_many(docs)
print(f"Inserted IDs: {insert_many_result.inserted_ids}")

# Создание уникального индекса
try:
    collection.create_index([("email", ASCENDING)], unique=True)
except DuplicateKeyError:
    print("Duplicate email found")

# ===== READ =====
# Найти один документ
one_doc = collection.find_one({"name": "John"})
print(f"One document: {one_doc}")

# Найти по ID
doc_by_id = collection.find_one({"_id": ObjectId(insert_one_result.inserted_id)})
print(f"Document by ID: {doc_by_id}")

# Найти несколько документов с фильтром
many_docs = collection.find({"age": {"$gt": 30}})
print("Documents with age > 30:")
for doc in many_docs:
    print(doc)

# Сортировка и лимит
sorted_docs = collection.find().sort("age", DESCENDING).limit(2)
print("Top 2 oldest:")
for doc in sorted_docs:
    print(doc)

# Подсчет документов
count = collection.count_documents({"city": "New York"})
print(f"Count NYC residents: {count}")

# Агрегация
pipeline = [
    {"$match": {"age": {"$gt": 25}}},
    {"$group": {"_id": "$city", "count": {"$sum": 1}}}
]
agg_result = collection.aggregate(pipeline)
print("Aggregation result:")
for res in agg_result:
    print(res)

# ===== UPDATE =====
# Обновить один документ
update_one_result = collection.update_one(
    {"name": "John"},
    {"$set": {"age": 31, "status": "active"}}
)
print(f"Matched: {update_one_result.matched_count}, Modified: {update_one_result.modified_count}")

# Обновить несколько документов
update_many_result = collection.update_many(
    {"age": {"$gt": 30}},
    {"$inc": {"age": 1}}
)
print(f"Updated many: {update_many_result.modified_count}")

# Обновить или вставить (upsert)
upsert_result = collection.update_one(
    {"name": "Eve"},
    {"$set": {"age": 28, "city": "Miami"}},
    upsert=True
)
print(f"Upserted ID: {upsert_result.upserted_id}")

# ===== DELETE =====
# Удалить один документ
delete_one_result = collection.delete_one({"name": "John"})
print(f"Deleted count: {delete_one_result.deleted_count}")

# Удалить несколько документов
delete_many_result = collection.delete_many({"age": {"$gt": 40}})
print(f"Deleted many: {delete_many_result.deleted_count}")

# Удалить все документы в коллекции (осторожно!)
# collection.delete_many({})

# ===== ДОПОЛНИТЕЛЬНО =====
# Текст поиск (требует текстового индекса)
# collection.create_index([("name", "text")])
# text_search = collection.find({"$text": {"$search": "John"}})

# Работа с вложенными документами
collection.update_one(
    {"name": "Alice"},
    {"$set": {"address": {"street": "Main", "number": 123}}}
)

# Работа с массивами
collection.update_one(
    {"name": "Bob"},
    {"$push": {"hobbies": "fishing"}}
)

# Закрытие соединения
client.close()
