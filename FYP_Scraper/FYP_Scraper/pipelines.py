# FYP_Scraper/FYP_Scraper/pipelines.py

import pymongo
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
from dotenv import load_dotenv
from urllib.parse import quote_plus
import os

class MongoDBPipeline:
    def __init__(self):
        load_dotenv()
        username = quote_plus(os.getenv("MONGODB_USERNAME"))
        password = quote_plus(os.getenv("MONGODB_PASSWORD"))
        self.mongo_uri = f"mongodb+srv://{username}:{password}@cluster0.66sawpl.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
        self.client = None
        self.db = None

    def open_spider(self, spider):
        try:
            self.client = pymongo.MongoClient(self.mongo_uri)
            self.db = self.client['news_db']
            spider.logger.info("Connected to MongoDB successfully!")
        except Exception as e:
            spider.logger.error(f"MongoDB Connection Error: {str(e)}")
            raise

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        
        required_fields = ['title', 'content', 'date', 'url', 'source']
        missing_fields = [field for field in required_fields 
                         if not adapter.get(field) or adapter.get(field).strip() == '']
        
        if missing_fields:
            error_msg = f"Missing or empty required fields: {', '.join(missing_fields)}"
            spider.logger.error(f"Item dropped - {error_msg}")
            spider.logger.error(f"URL: {adapter.get('url', 'No URL')}")
            raise DropItem(error_msg)

        try:
            source = adapter.get('source', 'unknown')
            collection_name = f"{source}_raw"
            
            collection = self.db[collection_name]
            
            collection.create_index([('url', pymongo.ASCENDING)], unique=True)
            
            result = collection.update_one(
                {'url': adapter['url']},
                {'$set': adapter.asdict()},
                upsert=True
            )
            
            if result.upserted_id:
                spider.logger.info(f"New article saved to {collection_name}: {adapter['url']}")
            else:
                spider.logger.info(f"Article updated in {collection_name}: {adapter['url']}")
                
            return item

        except pymongo.errors.DuplicateKeyError:
            spider.logger.warning(f"Duplicate article found: {adapter['url']}")
            raise DropItem(f"Duplicate article found: {adapter['url']}")
            
        except Exception as e:
            spider.logger.error(f"MongoDB Error processing {adapter.get('url', 'No URL')}: {str(e)}")
            raise DropItem(f"MongoDB Error: {str(e)}")

    def close_spider(self, spider):
        if self.client:
            self.client.close()
            spider.logger.info("MongoDB connection closed")
