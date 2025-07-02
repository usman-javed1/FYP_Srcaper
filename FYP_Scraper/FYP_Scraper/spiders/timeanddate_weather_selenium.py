import scrapy
from datetime import datetime
from ..items import WeatherDataItem
import re
import calendar
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time


class TimeAndDateWeatherSeleniumSpider(scrapy.Spider):
    name = "timeanddate_weather_selenium"
    allowed_domains = ["www.timeanddate.com"]
    
    # Enable weather pipeline for production
    custom_settings = {
        'ITEM_PIPELINES': {
            'FYP_Scraper.pipelines.WeatherMongoDBPipeline': 300,
        },
        'DOWNLOAD_DELAY': 2,
        'LOG_LEVEL': 'INFO',
        'FEEDS': {
            'weather_data_selenium.json': {
                'format': 'json',
                'encoding': 'utf8',
                'indent': 2,
            }
        }
    }
    
    def __init__(self, month=None, year=None, *args, **kwargs):
        super(TimeAndDateWeatherSeleniumSpider, self).__init__(*args, **kwargs)
        self.month = month or "12"
        self.year = year or "2015"
        self.location = "gujranwala"
        self.country = "pakistan"
        self.scraped_data = []
        self.driver = None
        
    def start_requests(self):
        """Start with the main weather page"""
        base_url = f"https://www.timeanddate.com/weather/{self.country}/{self.location}/historic?month={self.month}&year={self.year}"
        
        self.logger.info(f"Starting to scrape weather data for {self.location}, {self.month}/{self.year}")
        self.logger.info(f"Base URL: {base_url}")
        
        yield scrapy.Request(
            url=base_url,
            callback=self.parse_with_selenium,
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
        )

    def setup_driver(self):
        """Setup Chrome driver with options"""
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # Run in headless mode
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
        
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        self.driver.implicitly_wait(10)
        
    def parse_with_selenium(self, response):
        """Parse the page using Selenium to handle JavaScript"""
        try:
            self.setup_driver()
            
            # Navigate to the page
            self.driver.get(response.url)
            time.sleep(3)  # Wait for page to load
            
            # Get the number of days in the month
            month_int = int(self.month)
            year_int = int(self.year)
            num_days = calendar.monthrange(year_int, month_int)[1]
            
            self.logger.info(f"Scraping {num_days} days for {self.month}/{self.year}")
            
            # Extract location name
            try:
                location_element = self.driver.find_element(By.CSS_SELECTOR, 'h1')
                location = location_element.text.strip()
            except:
                location = f"{self.location.title()}, {self.country.title()}"
            
            # Find day navigation links
            day_links = self.driver.find_elements(By.CSS_SELECTOR, 'a[onclick*="cityssi"]')
            
            self.logger.info(f"Found {len(day_links)} day navigation links")
            
            # Process each day
            for day in range(1, num_days + 1):
                self.logger.info(f"Processing day {day}")
                
                # Find the link for this day
                day_link = None
                for link in day_links:
                    link_text = link.text.strip()
                    if re.search(rf'\b{day}\b', link_text):
                        day_link = link
                        break
                
                if day_link:
                    try:
                        # Click on the day link
                        self.driver.execute_script("arguments[0].click();", day_link)
                        time.sleep(2)  # Wait for content to load
                        
                        # Parse the weather data for this day
                        yield from self.parse_day_weather_data(day, location)
                        
                    except Exception as e:
                        self.logger.error(f"Error processing day {day}: {str(e)}")
                else:
                    self.logger.warning(f"Could not find link for day {day}")
            
        except Exception as e:
            self.logger.error(f"Error in Selenium parsing: {str(e)}")
        finally:
            if self.driver:
                self.driver.quit()
    
    def parse_day_weather_data(self, day_number, location):
        """Parse weather data for a specific day"""
        try:
            # Find all tables on the page
            tables = self.driver.find_elements(By.CSS_SELECTOR, 'table')
            
            self.logger.info(f"Day {day_number}: Found {len(tables)} tables")
            
            scraped_count = 0
            
            for table_idx, table in enumerate(tables):
                # Find all rows in the table
                rows = table.find_elements(By.CSS_SELECTOR, 'tr')
                
                for row_idx, row in enumerate(rows):
                    # Find all cells in the row
                    cells = row.find_elements(By.CSS_SELECTOR, 'td, th')
                    
                    # Look for rows with exactly 9 cells (daily weather data)
                    if len(cells) == 9:
                        try:
                            # Extract time and date from first cell
                            time_date_cell = cells[0].text.strip()
                            if not time_date_cell:
                                continue
                            
                            # Skip header rows
                            if time_date_cell.lower() in ['time', 'temp', 'weather', 'wind', 'humidity', 'barometer', 'visibility']:
                                continue
                            
                            # Check if this looks like a time/date (contains time)
                            if re.search(r'\d{2}:\d{2}', time_date_cell):
                                # Extract time from the time_date string
                                time_match = re.search(r'(\d{2}:\d{2})', time_date_cell)
                                time = time_match.group(1) if time_match else time_date_cell
                                
                                # Extract day number from time_date string if available
                                day_match = re.search(r'(\w+),\s*(\d+)\s+(\w+)', time_date_cell)
                                if day_match:
                                    day_name = day_match.group(1)
                                    extracted_day = day_match.group(2)
                                    month_name = day_match.group(3)
                                    date = f"{extracted_day} {month_name}"
                                    actual_day = extracted_day
                                else:
                                    # If no date in time string, use the day_number parameter
                                    date = f"Day {day_number}"
                                    actual_day = str(day_number)
                                
                                # Extract temperature from cell 2
                                temp_cell = cells[2].text.strip()
                                temp_high = temp_low = "N/A"
                                if temp_cell:
                                    temp_match = re.search(r'(\d+(?:\.\d+)?)\s*°C', temp_cell)
                                    if temp_match:
                                        temp_high = temp_low = temp_match.group(1)
                                
                                # Extract weather condition from cell 3
                                weather_condition = "N/A"
                                weather_cell = cells[3].text.strip()
                                if weather_cell:
                                    weather_condition = weather_cell
                                
                                # Extract wind speed from cell 4
                                wind_speed = "N/A"
                                wind_cell = cells[4].text.strip()
                                if wind_cell:
                                    wind_speed = wind_cell
                                
                                # Extract humidity from cell 6
                                humidity = "N/A"
                                humidity_cell = cells[6].text.strip()
                                if humidity_cell:
                                    humidity_match = re.search(r'(\d+)\s*%', humidity_cell)
                                    if humidity_match:
                                        humidity = humidity_match.group(1) + "%"
                                
                                # Extract pressure from cell 7
                                pressure = "N/A"
                                pressure_cell = cells[7].text.strip()
                                if pressure_cell:
                                    pressure_match = re.search(r'(\d+)\s*mbar', pressure_cell)
                                    if pressure_match:
                                        pressure = pressure_match.group(1) + " mbar"
                                
                                # Extract visibility from cell 8
                                visibility = "N/A"
                                visibility_cell = cells[8].text.strip()
                                if visibility_cell:
                                    visibility = visibility_cell
                                
                                # Create unique ID for the record
                                unique_id = f"{self.location}_{self.year}_{self.month}_{actual_day}_{time.replace(':', '')}"
                                
                                # Create weather data item
                                item = WeatherDataItem()
                                item['unique_id'] = unique_id
                                item['date'] = date
                                item['time'] = time
                                item['day_number'] = actual_day
                                item['temperature_high'] = temp_high
                                item['temperature_low'] = temp_low
                                item['weather_condition'] = weather_condition
                                item['humidity'] = humidity
                                item['wind_speed'] = wind_speed
                                item['pressure'] = pressure
                                item['visibility'] = visibility
                                item['location'] = location
                                item['month'] = self.month
                                item['year'] = self.year
                                item['url'] = self.driver.current_url
                                item['scraped_at'] = datetime.now().isoformat()
                                
                                # Store data for analysis
                                self.scraped_data.append({
                                    'day_number': actual_day,
                                    'time': time,
                                    'temperature': temp_high,
                                    'weather_condition': weather_condition,
                                    'url': self.driver.current_url,
                                    'time_date_raw': time_date_cell
                                })
                                
                                # Print the scraped data
                                print("=" * 60)
                                print(f"SCRAPED WEATHER DATA FOR DAY {actual_day}:")
                                print("=" * 60)
                                print(f"Unique ID: {unique_id}")
                                print(f"Date: {date}")
                                print(f"Time: {time}")
                                print(f"Day: {actual_day}")
                                print(f"Temperature: {temp_high}°C")
                                print(f"Weather Condition: {weather_condition}")
                                print(f"Wind Speed: {wind_speed}")
                                print(f"Humidity: {humidity}")
                                print(f"Pressure: {pressure}")
                                print(f"Visibility: {visibility}")
                                print(f"Location: {location}")
                                print(f"Raw time_date: {time_date_cell}")
                                print("=" * 60)
                                
                                scraped_count += 1
                                yield item
                        
                        except Exception as e:
                            self.logger.error(f"Error parsing row {row_idx}: {str(e)}")
                            continue
            
            self.logger.info(f"Scraped {scraped_count} weather records for day {day_number}")
            
        except Exception as e:
            self.logger.error(f"Error parsing weather data for day {day_number}: {str(e)}")
    
    def closed(self, reason):
        """Called when spider is closed - analyze the data"""
        print(f"\n{'='*80}")
        print("SPIDER ANALYSIS")
        print(f"{'='*80}")
        
        # Group data by day
        days_data = {}
        for record in self.scraped_data:
            day = record['day_number']
            if day not in days_data:
                days_data[day] = []
            days_data[day].append(record)
        
        print(f"Total records scraped: {len(self.scraped_data)}")
        print(f"Days with data: {len(days_data)}")
        
        for day, records in days_data.items():
            print(f"\nDay {day}: {len(records)} records")
            for record in records[:3]:  # Show first 3 records
                print(f"  - {record['time']}: {record['temperature']}°C, {record['weather_condition']}")
            if len(records) > 3:
                print(f"  ... and {len(records) - 3} more records")
        
        # Check for duplicate data across days
        print(f"\n{'='*80}")
        print("DUPLICATE ANALYSIS")
        print(f"{'='*80}")
        
        if len(days_data) > 1:
            first_day = list(days_data.keys())[0]
            first_day_records = days_data[first_day]
            
            for day, records in days_data.items():
                if day != first_day:
                    if len(records) == len(first_day_records):
                        # Check if all records are identical
                        identical = True
                        for i, record in enumerate(records):
                            if (record['time'] != first_day_records[i]['time'] or 
                                record['temperature'] != first_day_records[i]['temperature'] or
                                record['weather_condition'] != first_day_records[i]['weather_condition']):
                                identical = False
                                break
                        
                        if identical:
                            print(f"WARNING: Day {day} has identical data to Day {first_day}")
                        else:
                            print(f"Day {day} has different data from Day {first_day}")
                    else:
                        print(f"Day {day} has different number of records ({len(records)}) than Day {first_day} ({len(first_day_records)})") 