import scrapy
from datetime import datetime
from ..items import WeatherDataItem
import re
import calendar
import json


class TimeAndDateWeatherSpider(scrapy.Spider):
    name = "timeanddate_weather"
    allowed_domains = ["www.timeanddate.com"]
    
    # Temporarily disable MongoDB pipeline for testing
    custom_settings = {
        'ITEM_PIPELINES': {
            # 'FYP_Scraper.pipelines.WeatherMongoDBPipeline': 300,
        },
        'DOWNLOAD_DELAY': 2,  # Increased delay to be respectful
        'LOG_LEVEL': 'INFO',
        'FEEDS': {
            'weather_data_test.json': {
                'format': 'json',
                'encoding': 'utf8',
                'indent': 2,
            }
        }
    }
    
    def __init__(self, month=None, year=None, *args, **kwargs):
        super(TimeAndDateWeatherSpider, self).__init__(*args, **kwargs)
        self.month = month or "12"
        self.year = year or "2015"
        self.location = "gujranwala"
        self.country = "pakistan"
        self.scraped_data = []  # Store data for analysis
        
    def start_requests(self):
        """Generate URLs for each day of the month"""
        self.logger.info(f"Starting to scrape weather data for {self.location}, {self.month}/{self.year}")
        
        # Get the number of days in the month
        month_int = int(self.month)
        year_int = int(self.year)
        num_days = calendar.monthrange(year_int, month_int)[1]
        
        self.logger.info(f"Scraping {num_days} days for {self.month}/{self.year}")
        
        # Generate a request for each day
        for day in range(1, num_days + 1):
            day_url = f"https://www.timeanddate.com/weather/{self.country}/{self.location}/historic?month={self.month}&year={self.year}&day={day}"
            
            self.logger.info(f"Queuing day {day}: {day_url}")
            
            yield scrapy.Request(
                url=day_url,
                callback=self.parse_weather_data,
                cb_kwargs={'day_number': str(day)},
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Accept-Encoding': 'gzip, deflate',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                }
            )

    def parse_weather_data(self, response, day_number):
        """Parse weather data from a specific day page"""
        self.logger.info(f"Parsing weather data for day {day_number}")
        
        # Print the URL being processed
        print(f"\n{'='*80}")
        print(f"PROCESSING URL: {response.url}")
        print(f"TARGET DAY: {day_number}")
        print(f"{'='*80}")
        
        # Extract location name from the page
        location_element = response.css('h1::text').get()
        if location_element:
            location = location_element.strip()
        else:
            location = f"{self.location.title()}, {self.country.title()}"
        
        # Find all tables on the page
        all_tables = response.css('table')
        print(f"Day {day_number}: Found {len(all_tables)} tables on the page")
        
        scraped_count = 0
        
        for table_idx, table in enumerate(all_tables):
            # Extract rows from the table
            rows = table.css('tr')
            
            # Look for rows with 9 cells (daily weather data)
            for row_idx, row in enumerate(rows):
                # Check for both td and th elements
                cells = row.css('td, th')
                
                # Look for rows with exactly 9 cells (daily weather data)
                if len(cells) == 9:
                    try:
                        # Extract time and date from first cell
                        time_date_cell = cells[0].css('::text').get()
                        if not time_date_cell:
                            continue
                        
                        time_date = time_date_cell.strip()
                        
                        # Skip header rows
                        if time_date.lower() in ['time', 'temp', 'weather', 'wind', 'humidity', 'barometer', 'visibility']:
                            continue
                        
                        # Check if this looks like a time/date (contains time)
                        if re.search(r'\d{2}:\d{2}', time_date):
                            # Extract time from the time_date string
                            time_match = re.search(r'(\d{2}:\d{2})', time_date)
                            time = time_match.group(1) if time_match else time_date
                            
                            # Extract day number from time_date string if available
                            day_match = re.search(r'(\w+),\s*(\d+)\s+(\w+)', time_date)
                            if day_match:
                                day_name = day_match.group(1)
                                extracted_day = day_match.group(2)
                                month_name = day_match.group(3)
                                date = f"{extracted_day} {month_name}"
                                # Use extracted day if it matches our target day
                                if extracted_day == day_number:
                                    actual_day = extracted_day
                                else:
                                    actual_day = day_number
                            else:
                                # If no date in time string, use the day_number parameter
                                date = f"Day {day_number}"
                                actual_day = day_number
                            
                            # Extract temperature from cell 2
                            temp_cell = cells[2].css('::text').get()
                            temp_high = temp_low = "N/A"
                            if temp_cell:
                                temp_match = re.search(r'(\d+(?:\.\d+)?)\s*°C', temp_cell.strip())
                                if temp_match:
                                    temp_high = temp_low = temp_match.group(1)
                            
                            # Extract weather condition from cell 3
                            weather_condition = "N/A"
                            weather_cell = cells[3].css('::text').get()
                            if weather_cell:
                                weather_condition = weather_cell.strip()
                            
                            # Extract wind speed from cell 4
                            wind_speed = "N/A"
                            wind_cell = cells[4].css('::text').get()
                            if wind_cell:
                                wind_speed = wind_cell.strip()
                            
                            # Extract humidity from cell 6
                            humidity = "N/A"
                            humidity_cell = cells[6].css('::text').get()
                            if humidity_cell:
                                humidity_match = re.search(r'(\d+)\s*%', humidity_cell.strip())
                                if humidity_match:
                                    humidity = humidity_match.group(1) + "%"
                            
                            # Extract pressure from cell 7
                            pressure = "N/A"
                            pressure_cell = cells[7].css('::text').get()
                            if pressure_cell:
                                pressure_match = re.search(r'(\d+)\s*mbar', pressure_cell.strip())
                                if pressure_match:
                                    pressure = pressure_match.group(1) + " mbar"
                            
                            # Extract visibility from cell 8
                            visibility = "N/A"
                            visibility_cell = cells[8].css('::text').get()
                            if visibility_cell:
                                visibility = visibility_cell.strip()
                            
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
                            item['url'] = response.url
                            item['scraped_at'] = datetime.now().isoformat()
                            
                            # Store data for analysis
                            self.scraped_data.append({
                                'day_number': actual_day,
                                'time': time,
                                'temperature': temp_high,
                                'weather_condition': weather_condition,
                                'url': response.url,
                                'time_date_raw': time_date
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
                            print(f"Raw time_date: {time_date}")
                            print("=" * 60)
                            
                            scraped_count += 1
                            yield item
                    
                    except Exception as e:
                        self.logger.error(f"Error parsing row {row_idx}: {str(e)}")
                        continue
        
        self.logger.info(f"Scraped {scraped_count} weather records for day {day_number}")
        self.logger.info(f"Completed scraping weather data for day {day_number}")
    
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