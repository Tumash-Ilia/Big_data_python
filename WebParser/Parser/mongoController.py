import json
from pymongo import MongoClient


client = MongoClient('localhost', 27017)
db = client.dpbcviceni
collection = db.articles


def load_data():
    """
    Load data to mongoDB
    :return:
    """
    with open("data.json", "r") as read_file:
        json_data = read_file.read()
        myDict = json.loads(json_data)
        collection.insert_many(myDict)

def find_all_titles():
    """
    Find all titles
    :return:
    """
    res = collection.find({}, {'Title': 1, "_id": 0})
    # for r in res:
    #     print(r)
    print(res[0]['Title'])
    print(res.count())

def delete_empty():
    res = collection.delete_many({'Title': ""})
    print(res.deleted_count)

if __name__ == '__main__':
    load_data()
    delete_empty()
    find_all_titles()
    client.close()
