# Weather Data Scraper - Selenium Version

This is a Selenium-based web scraper for extracting historical weather data from timeanddate.com for Gujranwala, Pakistan.

## Features

- **Dynamic Content Handling**: Uses Selenium WebDriver to handle JavaScript-based dynamic content
- **Day-by-Day Navigation**: Properly navigates through different days using JavaScript functions
- **Comprehensive Data Extraction**: Extracts temperature, weather conditions, humidity, wind speed, pressure, and visibility
- **Unique Record Identification**: Creates unique IDs for each weather record
- **MongoDB Integration**: Stores data in MongoDB with proper indexing
- **JSON Export**: Also exports data to JSON files for backup/analysis

## Problem Solved

The original spider was scraping the same data for all days because:
1. The website uses JavaScript functions like `cityssi(8)` to dynamically load different days' data
2. URL parameters with different days were not actually loading different content
3. The content changes dynamically when clicking on day navigation links

This Selenium version properly:
- Loads the page with JavaScript enabled
- Finds and clicks on day navigation links
- Waits for content to load after each click
- Extracts the correct data for each specific day

## Requirements

```bash
pip install scrapy selenium webdriver-manager pymongo python-dotenv
```

## Usage

### Method 1: Direct Scrapy Command

```bash
cd FYP_Scraper
scrapy crawl timeanddate_weather_selenium -a month=12 -a year=2015
```

### Method 2: Using Test Script

```bash
cd FYP_Scraper
python test_weather_selenium.py 12 2015
```

### Parameters

- `month`: Month number (1-12)
- `year`: Year (e.g., 2015)

## Data Structure

Each weather record contains:

```json
{
  "unique_id": "gujranwala_2015_12_25_1400",
  "date": "25 Dec",
  "time": "14:00",
  "day_number": "25",
  "temperature_high": "16",
  "temperature_low": "16",
  "weather_condition": "Scattered clouds.",
  "humidity": "42%",
  "wind_speed": "No wind",
  "pressure": "1021 mbar",
  "visibility": "2 km",
  "location": "Past Weather in Gujranwala, Pakistan — December 2015",
  "month": "12",
  "year": "2015",
  "url": "https://www.timeanddate.com/weather/pakistan/gujranwala/historic?month=12&year=2015",
  "scraped_at": "2025-06-19T20:22:32.123456"
}
```

## Database Storage

Data is stored in MongoDB with:
- **Collection**: `weather_data`
- **Unique Index**: On `unique_id` field to prevent duplicates
- **Database**: Configured via environment variables

## Output Files

- `weather_data_selenium.json`: JSON export of scraped data
- MongoDB: Primary storage with proper indexing

## Performance

- **Speed**: ~2-3 seconds per day (includes JavaScript loading time)
- **Reliability**: Handles dynamic content loading properly
- **Scalability**: Can scrape multiple months/years sequentially

## Example Results

For December 2015:
- **Total Records**: 164 weather records
- **Days Covered**: 31 days
- **Records per Day**: 2-8 records (varies by day)
- **Time Slots**: Various times throughout each day (02:00, 05:00, 08:00, 11:00, 14:00, 17:00, 20:00, 23:00)

## Troubleshooting

### Common Issues

1. **Chrome Driver Issues**: The spider automatically downloads the correct Chrome driver version
2. **JavaScript Loading**: The spider waits for content to load after each day click
3. **Memory Usage**: Selenium uses more memory than regular Scrapy, but handles dynamic content properly

### Error Handling

- Automatic retry for failed requests
- Graceful handling of missing data
- Detailed logging for debugging

## Comparison with Original Spider

| Feature | Original Spider | Selenium Spider |
|---------|----------------|-----------------|
| JavaScript Support | ❌ No | ✅ Yes |
| Dynamic Content | ❌ No | ✅ Yes |
| Day Navigation | ❌ Same data | ✅ Different data per day |
| Data Accuracy | ❌ Duplicate data | ✅ Accurate per day |
| Performance | ✅ Fast | ⚠️ Slower (JS loading) |
| Reliability | ❌ Low | ✅ High |

## Future Enhancements

1. **Parallel Processing**: Run multiple months concurrently
2. **Data Validation**: Add validation for weather data consistency
3. **Error Recovery**: Resume from last successful day if interrupted
4. **API Integration**: Create REST API for data access

## License

This project is part of the FYP Scraper system. 