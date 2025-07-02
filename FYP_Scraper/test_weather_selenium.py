#!/usr/bin/env python3
"""
Test script for the Selenium-based weather spider
"""

import subprocess
import sys
import os

def run_weather_spider(month, year):
    """Run the weather spider with specified month and year"""
    
    # Change to the project directory
    project_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(project_dir)
    
    # Build the command
    cmd = [
        'scrapy', 'crawl', 'timeanddate_weather_selenium',
        '-a', f'month={month}',
        '-a', f'year={year}',
        '-s', 'LOG_LEVEL=INFO'
    ]
    
    print(f"Running weather spider for {month}/{year}...")
    print(f"Command: {' '.join(cmd)}")
    print("-" * 60)
    
    try:
        # Run the spider
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)  # 30 minute timeout
        
        # Print output
        if result.stdout:
            print("STDOUT:")
            print(result.stdout)
        
        if result.stderr:
            print("STDERR:")
            print(result.stderr)
        
        print(f"Exit code: {result.returncode}")
        
        if result.returncode == 0:
            print(f"✅ Successfully scraped weather data for {month}/{year}")
        else:
            print(f"❌ Failed to scrape weather data for {month}/{year}")
            
    except subprocess.TimeoutExpired:
        print(f"❌ Spider timed out after 30 minutes for {month}/{year}")
    except Exception as e:
        print(f"❌ Error running spider: {str(e)}")

if __name__ == "__main__":
    # Default values
    month = "12"
    year = "2015"
    
    # Check command line arguments
    if len(sys.argv) >= 2:
        month = sys.argv[1]
    if len(sys.argv) >= 3:
        year = sys.argv[2]
    
    print(f"Weather Data Scraper - Selenium Version")
    print(f"Target: Gujranwala, Pakistan")
    print(f"Period: {month}/{year}")
    print("=" * 60)
    
    run_weather_spider(month, year) 