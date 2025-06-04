# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from w3lib.html import remove_tags
from datetime import datetime
from itemloaders.processors import TakeFirst, MapCompose, Join


def clean_text(text):
    if text:
        return remove_tags(text).strip()
    return ''

class NewsArticleItem(scrapy.Item):
    title = scrapy.Field(
        input_processor=MapCompose(str.strip),
        output_processor=TakeFirst())

    content = scrapy.Field(
        input_processor=MapCompose(clean_text),
        output_processor=Join()
    )

    date = scrapy.Field(
        input_processor=MapCompose(str.strip),
        output_processor=TakeFirst()
    )

    url = scrapy.Field(
        output_processor=TakeFirst()
    )

    source = scrapy.Field(
        input_processor=MapCompose(str.strip),
        output_processor=TakeFirst()
    )

    reported_time = scrapy.Field(
        input_processor=MapCompose(str.strip),
        output_processor=TakeFirst()
    )
    
    category = scrapy.Field(
        input_processor=MapCompose(str.strip),
        output_processor=TakeFirst(),
        default='N/A'
    )

