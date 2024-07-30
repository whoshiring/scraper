# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.htm
import logging
import pymongo
from itemadapter import ItemAdapter


class MongoPipeline:
    collection_name = "jobs"

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        return cls(
            mongo_uri=crawler.settings.get("MONGO_URI"),
            mongo_db=crawler.settings.get("MONGO_DATABASE", "items"),
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        # Transform the id field to listing_id
        if "id" in item:
            item["listing_id"] = item.pop("id")
        # Insert the item into the MongoDB collection
        self.db[self.collection_name].insert_one(ItemAdapter(item).asdict())
        return item
