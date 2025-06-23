import scrapy
from urllib.parse import urljoin
from scrapy.http import Request
from scrapy.loader import ItemLoader
from FYP_Scraper.items import NewsArticleItem
import re

class JangNewsSpider(scrapy.Spider):
    name = "jang_news"
    allowed_domains = ["jang.com.pk"]
    start_urls = ["https://jang.com.pk/category/magazine/jurm-o-saza"]
    scraped_count = 0
    skipped_count = 0
    current_offset = 2  # Adjusted to start at 2 based on button data
    custom_settings = {
        'FEED_EXPORT_ENCODING': 'utf-8',
        'FEED_EXPORT_FIELDS': ['title', 'date', 'url', 'content', 'category', 'source', 'reported_time'],
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
        'DOWNLOAD_DELAY': 2,
        'DUPEFILTER_DEBUG': True,
        'FEED_URI': 'jangNews.csv',
        'FEED_FORMAT': 'csv',
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'RETRY_TIMES': 3,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 522, 524, 408, 429, 403, 0],
        'RETRY_DELAY': 5,
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': 590,
        },
        'COMPRESSION_ENABLED': True,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.crawled_urls = set()

    def parse(self, response):
        self.logger.info(f"Content-Type: {response.headers.get('Content-Type')}")
        self.logger.info(f"Response body (first 100 chars): {response.body[:100]}")
        if not response.body or b'text/html' not in response.headers.get('Content-Type', b''):
            self.logger.warning(f"Non-HTML response skipped: {response.url}")
            return
        yield from self.extract_articles(response)
        yield from self.request_next_page(response)

    def request_next_page(self, response):
        params = {
            'parent_slug': 'magazine',
            'child_slug': 'jurm-o-saza',
            'category_id': '116',
            'offset': str(self.current_offset)
        }
        headers = {
            'User-Agent': self.custom_settings['USER_AGENT'],
            'Referer': response.url,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'X-Requested-With': 'XMLHttpRequest',
        }
        self.current_offset += 2  # Increment by 2 based on button behavior
        yield Request(
            url="https://jang.com.pk/category/load_more_subcategories",
            params=params,
            headers=headers,
            callback=self.parse_ajax,
            meta={'offset': self.current_offset}
        )

    def parse_ajax(self, response):
        offset = response.meta.get('offset', 'unknown')
        self.logger.info(f"Content-Type (AJAX): {response.headers.get('Content-Type')}")
        if not response.body or b'text/html' not in response.headers.get('Content-Type', b''):
            self.logger.info(f"⏹️ Non-HTML response at offset {offset}. Stopping pagination.")
            return
        links = response.css('li a::attr(href)').getall()
        self.logger.debug(f"Found links: {links}")  # Debug log
        unique_links = set(urljoin(response.url, link) for link in links if link and '/news/' in link)
        
        if not unique_links:
            self.logger.info(f"⏹️ No more articles found at offset {offset}. Stopping pagination.")
            return

        new_links = [link for link in unique_links if link not in self.crawled_urls]
        if not new_links:
            self.logger.info(f"⏹️ All links at offset {offset} are duplicates. Stopping pagination.")
            return
        self.crawled_urls.update(unique_links)

        for link in new_links:
            if '/news/' in link:
                yield Request(url=link, callback=self.parse_article)

        yield from self.request_next_page(response)

    def extract_articles(self, response):
        if not response.body or b'text/html' not in response.headers.get('Content-Type', b''):
            self.logger.warning(f"Non-HTML response skipped: {response.url}")
            return
        article_links = response.css('li a::attr(href)').getall()
        unique_links = set(urljoin(response.url, link) for link in article_links if link and '/news/' in link)
        for link in unique_links:
            if '/news/' in link:
                yield Request(url=link, callback=self.parse_article)

    def parse_article(self, response):
        if not response.body or b'text/html' not in response.headers.get('Content-Type', b''):
            self.logger.warning(f"Non-HTML response skipped: {response.url}")
            return
        loader = ItemLoader(item=NewsArticleItem(), response=response)

        # Improved title extraction with fallback
        title = response.css('h1::text').get() or response.css('div.main-heading h3::text').get()
        self.logger.debug(f"Title found: {title}")  # Debug log
        if not title:
            self.logger.warning(f"⏩ Skipped (no title): {response.url}")
            self.skipped_count += 1
            return

        # Improved content extraction with fallback
        paragraphs = response.css('div.detail_view_content p:not(:empty)::text').getall() or \
                     response.css('div.main-content p::text').getall()
        content = ' '.join([p.strip() for p in paragraphs if p.strip()]) if paragraphs else 'N/A'
        date_text = response.css('div.detail-time::text').get() or response.css('div.cat-time::text').get()

        date_value = 'N/A'
        reported_time_value = 'N/A'
        if date_text:
            try:
                date_value = date_text.strip()
            except Exception as e:
                self.logger.warning(f"⚠️ Failed to parse date: {date_text}, Error: {e}")

        if not content:
            self.logger.warning(f"⚠️ No content found: {response.url}")

        loader.add_value('date', date_value)
        loader.add_value('title', title)
        loader.add_value('url', response.url)
        loader.add_value('content', content)
        loader.add_value('category', 'N/A')
        loader.add_value('source', 'jang_news')
        loader.add_value('reported_time', reported_time_value)

        item = loader.load_item()
        self.logger.info(f"✅ Article scraped: {response.url}, Item: {item}")
        self.scraped_count += 1
        yield item

    def closed(self, reason):
        self.logger.info("🕒 Spider closed")
        self.logger.info(f"✅ Total articles scraped: {self.scraped_count}")
        self.logger.info(f"⏩ Total skipped: {self.skipped_count}")
        self.logger.info(f"📑 Reason: {reason}")