import scrapy
from urllib.parse import urljoin
from scrapy.http import FormRequest
from scrapy.loader import ItemLoader
from FYP_Scraper.items import NewsArticleItem
import re

class DunyaNewsSpider(scrapy.Spider):
    name = "dunya_news"
    allowed_domains = ["urdu.dunyanews.tv"]
    start_urls = ["https://urdu.dunyanews.tv/index.php/ur/Crime"]
    scraped_count = 0
    skipped_count = 0
    current_page = 1
    custom_settings = {
        'FEED_EXPORT_ENCODING': 'utf-8',
        'FEED_EXPORT_FIELDS': ['title', 'date', 'url', 'content', 'category', 'source', 'reported_time'],
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'DOWNLOAD_DELAY': 2,
        'DUPEFILTER_DEBUG': True,
        'FEED_URI': 'dunyaNews.csv',
        'FEED_FORMAT': 'csv',
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
    }

    def parse(self, response):
        yield from self.extract_articles(response)
        yield from self.request_next_page(response)

    def request_next_page(self, response):
        payload = {
            'page': str(self.current_page + 1),
            'catid': '7',  # Crime category ID, verify via network inspection
            'tag': ''
        }
        headers = {
            'User-Agent': self.custom_settings['USER_AGENT'],
            'Referer': response.url,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        }
        self.logger.debug(f"🔍 Requesting AJAX page {self.current_page + 1} with payload: {payload}")
        yield FormRequest(
            url="https://urdu.dunyanews.tv/index.php?option=com_dunyanews&task=ajax&action=loadMoreNews",
            formdata=payload,
            headers=headers,
            callback=self.parse_ajax,
            meta={'page': self.current_page + 1}
        )

    def parse_ajax(self, response):
        page = response.meta.get('page', 'unknown')
        self.logger.debug(f"📡 AJAX Response for page {page}: {response.text[:500]}...")
        links = set()  # Use a set to avoid duplicates
        try:
            data = response.json()  # Attempt to parse as JSON
            links.update(urljoin(response.url, item.get('url', '')) for item in data.get('articles', []) if 'Crime' in item.get('url', ''))
            self.logger.debug(f"JSON parsed, found {len(links)} unique links")
        except ValueError:
            soup = response.css('div.newsBox a::attr(href)')  # Updated selector
            links.update(urljoin(response.url, link.get()) for link in soup if link.get() and 'Crime' in link.get())
            self.logger.debug(f"HTML parsed, found {len(links)} unique links")

        if not links:
            self.logger.info(f"⏹️ No more articles found on page {page}. Stopping pagination.")
            return

        for link in links:
            if 'Crime' in link and '/index.php/ur/Crime/' in link:
                self.logger.debug(f"🔗 Found AJAX URL: {link}")
                yield scrapy.Request(url=link, callback=self.parse_article)

        self.current_page += 1
        yield from self.request_next_page(response)

    def extract_articles(self, response):
        article_links = [urljoin(response.url, link) for link in response.css('div.newsBox a::attr(href)').getall()]
        self.logger.debug(f"🌐 Initial page: Found {len(article_links)} links")
        for link in article_links:
            if 'Crime' in link and '/index.php/ur/Crime/' in link:
                self.logger.debug(f"🔗 Found URL: {link}")
                yield scrapy.Request(url=link, callback=self.parse_article)

    def parse_article(self, response):
        loader = ItemLoader(item=NewsArticleItem(), response=response)

        title = response.css('h1::text').get()
        paragraphs = response.css('div.main-news p:not(:empty)::text').getall()
        content = ' '.join([p.strip() for p in paragraphs if p.strip()])
        date_text = response.css('div.newsdate::text').get()

        date_value = 'N/A'
        reported_time_value = 'N/A'
        if date_text:
            try:
                match = re.search(r'Published On (\d+ \w+, \d{4})\s*(\d{2}:\d{2} [ap]m)', date_text.strip())
                if match:
                    date_value = match.group(1).strip()  # e.g., "23 June, 2025"
                    reported_time_value = match.group(2).strip()  # e.g., "09:15 am"
            except Exception as e:
                self.logger.warning(f"⚠️ Failed to parse date/time: {date_text}, Error: {e}")

        if not title or '/Crime/' not in response.url:
            self.logger.warning(f"⏩ Skipped (not crime-related): {response.url}")
            self.skipped_count += 1
            return

        if not content:
            self.logger.warning(f"⚠️ No content found: {response.url}")

        loader.add_value('date', date_value)
        loader.add_value('title', title)
        loader.add_value('url', response.url)
        loader.add_value('content', content or 'N/A')
        loader.add_value('category', 'N/A')
        loader.add_value('source', 'dunya_news')
        loader.add_value('reported_time', reported_time_value)

        self.logger.info(f"✅ Article scraped: {response.url}")
        self.scraped_count += 1
        yield loader.load_item()

        related_links = [urljoin(response.url, link) for link in response.css('div.related-news a::attr(href)').getall()]
        for link in related_links:
            if 'Crime' in link and '/index.php/ur/Crime/' in link:
                self.logger.debug(f"🔗 Found related URL: {link}")
                yield scrapy.Request(url=link, callback=self.parse_article)

    def closed(self, reason):
        self.logger.info("🕒 Spider closed")
        self.logger.info(f"✅ Total articles scraped: {self.scraped_count}")
        self.logger.info(f"⏩ Total skipped: {self.skipped_count}")
        self.logger.info(f"📑 Reason: {reason}")