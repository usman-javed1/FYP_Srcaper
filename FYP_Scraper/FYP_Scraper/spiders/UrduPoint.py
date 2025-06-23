import scrapy
from scrapy.http import FormRequest, Request
from datetime import datetime
from FYP_Scraper.items import NewsArticleItem
import json, os, re
from scrapy.exceptions import CloseSpider

class UrduPointMultiCategorySpider(scrapy.Spider):
    name = "urdupoint_multi_category"
    allowed_domains = ["urdupoint.com"]
    ajax_url = "https://www.urdupoint.com/daily/ajax_lmore.php"
    state_file = "scraper_state.json"
    scraped_urls_file = "scraped_urls.txt"

    def __init__(self):
        super().__init__()
        self.categories = [
            {"name": "murder", "tid": "MzR_-md7UkFT-dRaP7bBneWjG6l6k8ZI-PgEXnFKGi4", "keywords": ["قتل", "ہلاک", "قتل کیس", "فائرنگ", "خودکشی", "ملزم", "گرفتار", "سزائے موت"]},
            {"name": "thief", "tid": "AqpURk9gIAWriZekSAlWjFTm6-JaAIpFg21TsAL_bdc", "keywords": ["چور", "چوری"]},
            {"name": "robbery", "tid": "peViPq0AEwKA9NQMqpjkliiX1KYWdmvNYXx8rMyFo7s", "keywords": ["ڈکیتی", "چوری", "رابری"]},
            {"name": "kidnapping", "tid": "lzBfUUr4dDvlyOqI-NCCjTdubYqtIXDwZ4HrInBHhD4", "keywords": ["اغوا", "کناپنگ", "اغواء کار"]},
            {"name": "suicide", "tid": "PR_gUXYbPn_5ocdO4-Sfm2tR452QYA-RGj9AlxVTr2M", "keywords": ["خودکشی", "اپنی جان لینا"]},
            {"name": "terrorist", "tid": "-QHTdT8y3rRt9jlraC2q1zQBUwOvu6txnhCiQ76p4-k", "keywords": ["دہشت گرد", "دہشت گردی"]},
        ]
        self.geopolitical_keywords = ["امریکہ", "ایران", "اسرائیل", "غزہ", "حماس", "یوکرین", "جنگ", "روس", "فلسطین", "نیتن یاہو", "طالبان", "افغانستان", "بھارت", "سرحد", "عالمی", "بیرون ملک", "بین الاقوامی"]
        self.pakistan_locations = [
         # National terms
         "پاکستان", "پنجاب", "سندھ", "بلوچستان", "خیبر پختونخوا", "گلگت بلتستان", "آزاد کشمیر",

          # Major cities (All provinces)
         "کراچی", "لاہور", "اسلام آباد", "پشاور", "کوئٹہ", "فیصل آباد", "راولپنڈی", "ملتان",
         "گوجرانوالہ", "حیدرآباد", "سرگودھا", "سیالکوٹ", "بہاولپور", "سکھر", "ساہیوال", "اوکاڑہ",
         "مظفرآباد", "ٹوبہ ٹیک سنگھ", "خضدار", "مردان", "نوشہرہ", "خانیوال", "لیہ", "چکوال",
         "ڈیرہ غازی خان", "جہلم", "منڈی بہاؤالدین", "نوابشاہ", "مٹیاری", "لورالائی", "رحیم یار خان",

         # More Punjab cities/districts
         "قصور", "شیخوپورہ", "ننکانہ صاحب", "بھکر", "چنیوٹ", "لودھراں", "وہاڑی", "بہاولنگر",
         "راجن پور", "مظفر گڑھ", "حافظ آباد", "پاکپتن", "جھنگ", "خوشاب", "میانوالی", "سرائے عالمگیر",
         "شورکوٹ", "کبیروالا", "جلال پور جٹاں", "کمالیہ", "چوآ سعیدن شاہ", "رینالہ خورد", "احمد پور شرقیہ",
         "شکرگڑھ", "ظفر وال", "نارووال", "ڈسکہ", "پھالیہ", "کندیاں", "صفدر آباد", "فورٹ عباس",
 
         # More KPK
        "چارسدہ", "صوابی", "بٹگرام", "شانگلہ", "سوات", "دیر", "چترال", "کوہاٹ", "بنوں",
         "لکی مروت", "ٹانک", "ہنگو", "کرک", "ڈیرہ اسماعیل خان", "مانسہرہ", "ایبٹ آباد", "ہری پور",
         "تورغر", "بونیر", "مالاکنڈ",

         # More Sindh
        "ٹنڈو الہ یار", "ٹنڈو محمد خان", "عمرکوٹ", "بدین", "تھرپارکر", "دادو", "سانگھڑ",
        "میرپور خاص", "خیرپور", "شکارپور", "لاڑکانہ", "جیکب آباد", "کندھ کوٹ", "گھوٹکی", "کشمور",
        "ٹھٹھہ", "سجاول", "حب", "کنری", "مٹیاری",
 
        # More Balochistan
        "قلات", "چمن", "ژوب", "سبی", "نصیر آباد", "جعفرآباد", "بارکھان", "آواران", "خاران",
        "پنجگور", "تربت", "گوادر", "قلعہ سیف اللہ", "قلعہ عبداللہ", "ڈیرہ مراد جمالی", "زیارت",
        "نوشکی", "موسیٰ خیل", "پشین", "دالبندین",

         # Gilgit-Baltistan
        "گلگت", "سکردو", "ہنزہ", "غذر", "گانچھے", "دیامر", "استور", "نگر", "چلاس",
   
          # Azad Jammu and Kashmir
        "مظفرآباد", "باغ", "راولا کوٹ", "میرپور", "کوٹلی", "ہٹیاں بالا", "سدھنوتی", "نیلم",

         # Other/Federal/Borderline
        "مری", "ٹیکسلا", "حسن ابدال", "کوٹ ادو", "جتوئی", "چونیاں", "وزیر آباد", "گجرات", "پسرور"
      ]

        self.current_category_index = 0
        self.page = 1
        self.scraped_urls = set()
        self.load_state()
        self.load_scraped_urls()

    def load_state(self):
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    state = json.load(f)
                    self.current_category_index = min(state.get('category_index', 0), len(self.categories) - 1)
                    self.page = max(state.get('page', 1), 1)
                    self.logger.info(f"Resumed state: Category {self.categories[self.current_category_index]['name']}, Page {self.page}")
            except Exception as e:
                self.logger.warning(f"State load failed, starting fresh: {e}")

    def save_state(self):
        state = {
            'category_index': self.current_category_index,
            'page': self.page,
            'timestamp': datetime.now().isoformat()
        }
        try:
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(state, f, ensure_ascii=False, indent=2)
            self.logger.info(f"State saved: {state}")
        except Exception as e:
            self.logger.warning(f"Failed to save state: {e}")

    def load_scraped_urls(self):
        if os.path.exists(self.scraped_urls_file):
            try:
                with open(self.scraped_urls_file, 'r', encoding='utf-8') as f:
                    self.scraped_urls = set(line.strip() for line in f if line.strip())
                self.logger.info(f"Loaded {len(self.scraped_urls)} scraped URLs.")
            except Exception as e:
                self.logger.warning(f"Failed to load scraped URLs: {e}")

    def save_scraped_url(self, url):
        try:
            with open(self.scraped_urls_file, 'a', encoding='utf-8') as f:
                f.write(url + '\n')
            self.scraped_urls.add(url)
        except Exception as e:
            self.logger.warning(f"Failed to save URL: {url} — {e}")

    def start_requests(self):
        if self.current_category_index >= len(self.categories):
            raise CloseSpider("All categories completed")
        category = self.categories[self.current_category_index]
        yield FormRequest(
            url=self.ajax_url,
            formdata={"act": "get_more_tag_news", "tid": category['tid'], "m": str(self.page)},
            callback=self.parse_ajax,
            meta={"category": category['name'], "keywords": category['keywords']},
            errback=self.handle_error,
            dont_filter=True
        )

    def handle_error(self, failure):
        request = failure.request
        retries = request.meta.get('retry_count', 0)
        if retries < 3:
            request.meta['retry_count'] = retries + 1
            self.logger.info(f"Retrying {request.url} ({retries + 1})")
            yield request
        else:
            self.logger.warning(f"Skipping request after 3 retries: {request.url}")
            self.page += 1
            self.save_state()
            yield from self.start_requests()

    def parse_ajax(self, response):
        category = response.meta['category']
        keywords = response.meta['keywords']
        if not response.text.strip().startswith('{'):
            self.logger.error("Non-JSON response, skipping page")
            self.page += 1
            self.save_state()
            yield from self.start_requests()
            return
        try:
            json_data = response.json()
            html_response = scrapy.Selector(text=json_data.get("data", ""))
            articles = html_response.css("li.item_shadow")

            if not articles:
                self.logger.info(f"No more articles in {category}, moving to next.")
                self.current_category_index += 1
                self.page = 1
                self.save_state()
                yield from self.start_requests()
                return

            for article in articles:
                url = article.css("a::attr(href)").get()
                if not url or url in self.scraped_urls:
                    continue

                date_parts = article.css("div.item_date *::text").getall()
                title_date = " ".join(t.strip() for t in date_parts if t.strip())
                match = re.search(r"(\d{1,2}) (\w+) (\d{4})", title_date)
                if not match:
                    continue
                day, month_urdu, year = match.groups()
                month_map = {
                    "جنوری": "Jan", "فروری": "Feb", "مارچ": "Mar", "اپریل": "Apr",
                    "مئی": "May", "جون": "Jun", "جولائی": "Jul", "اگست": "Aug",
                    "ستمبر": "Sep", "اکتوبر": "Oct", "نومبر": "Nov", "دسمبر": "Dec"
                }
                date_str = f"{day} {month_map.get(month_urdu, 'Unknown')} {year}"
                try:
                    parsed_date = datetime.strptime(date_str, "%d %b %Y")
                except ValueError:
                    continue

                if not (datetime(2015, 1, 1) <= parsed_date <= datetime.now()):
                    continue

                # Extract reported_time (e.g., "12:34" or similar)
                reported_time = next((t.strip() for t in date_parts if ":" in t.strip()), "N/A")  # New: Extract time from date_parts

                yield Request(
                    url=url,
                    callback=self.parse_article,
                    meta={
                        "url": url,
                        "date": date_str,
                        "reported_time": reported_time,  # New: Pass reported_time in meta
                        "category": category,
                        "keywords": keywords
                    },
                    errback=self.handle_error,
                    dont_filter=True
                )

            self.page += 1
            self.save_state()
            yield from self.start_requests()

        except Exception as e:
            self.logger.error(f"JSON parse error: {e}")
            self.page += 1
            self.save_state()
            yield from self.start_requests()

    def parse_article(self, response):
        url = response.meta["url"]
        date = response.meta["date"]
        reported_time = response.meta["reported_time"]  # New: Retrieve reported_time from meta
        category = response.meta["category"]
        keywords = response.meta["keywords"]
        title = response.css("h1.urdu::text").get(default="").strip()
        # Remove unwanted tags like script and ad banners
        for sel in response.css("div.detail_txt.urdu script, div.detail_txt.urdu .ad, div.detail_txt.urdu [id^='gpt-']"):
            sel.root.getparent().remove(sel.root)
        
        raw_content = response.css("div.detail_txt.urdu *::text").getall()
        content = " ".join(t.strip() for t in raw_content if t.strip())
        # Optionally remove any leftover googletag commands
        content = re.sub(r'googletag\.cmd\.push\([^)]*\);', '', content)

        if not any(k in title or k in content for k in keywords):
            return
        if any(k in title or k in content for k in self.geopolitical_keywords):
            return
        if not any(loc in title or loc in content for loc in self.pakistan_locations):
            return

        self.save_scraped_url(url)
        item = NewsArticleItem()
        item["url"] = url
        item["date"] = date
        item["title"] = title
        item["content"] = content
        item["source"] = "urdupoint"
        item["reported_time"] = reported_time  # New: Set reported_time in the item
        item["category"] = category
        yield item