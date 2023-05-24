from pymongo import MongoClient

client = MongoClient('localhost', 27017)
client.list_database_names()


db = client.training
collection = db.marks

docs = collection.find()
for document in docs:
    print(document)



db.marks.aggregate([{"$group":{"_id":{"name":"$name","subject":"$subject"},"min":{"$min":"$marks"}}},{"$sort":{"_id.name":-1}}])
db.marks.aggregate([{"$group":{"_id":"$subject","total":{"$max":"$marks"}}}])
db.marks.aggregate([ {"$group":{"_id":"$subject","average":{"$avg":"$marks"}} }, {"$sort":{"average":-1} }, {"$limit":2}])