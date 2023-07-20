from pymongo import MongoClient
import json

client = MongoClient('localhost', 27017)
client.list_database_names()


# Open the original file and load the data
with open('movies.json', 'r') as f:
    data = json.load(f)

# Extract the 'doc' objects
docs = [item['doc'] for item in data['rows']]

# Write the 'doc' objects to a new file
with open('movies_transformed.json', 'w') as f:
    json.dump(docs, f)

# db = client.training
# collection = db.marks

# docs = collection.find()
# for document in docs:
#     print(document)



# db.marks.aggregate([{"$group":{"_id":{"name":"$name","subject":"$subject"},"min":{"$min":"$marks"}}},{"$sort":{"_id.name":-1}}])
# db.marks.aggregate([{"$group":{"_id":"$subject","total":{"$max":"$marks"}}}])
# db.marks.aggregate([ {"$group":{"_id":"$subject","average":{"$avg":"$marks"}} }, {"$sort":{"average":-1} }, {"$limit":2}])