# Real-Estate-Price-Scraper
This repository contains a Python script for collecting and processing real estate prices from the OLX website using a multiprocessing approach. The project leverages Object-Oriented Programming (OOP) principles to structure code efficiently and make it easily extendable.

## Key Features:
- **OOP Design:** Modular implementation using `City`, `Market`, and `Runner` classes for streamlined processing.
- **Concurrency:** Utilizes both `ThreadPoolExecutor` and `ProcessPoolExecutor` for efficient scraping and data processing.
- **API Integration:** Fetches real estate data directly via OLX's API.
- **Data Transformation:** Processes raw data into structured Excel sheets with filtering, cleaning, and key metrics computation (e.g., price per square meter).
- **Dynamic Currency Conversion:** Converts prices into USD based on the current exchange rate fetched from an external API.
- **Extensibility:** Easily adaptable to new markets, cities, or categories.

## Highlights:
- Handles multi-page scraping with automatic chunking and parallel processing.
- Includes filters to clean and refine scraped data for analysis.
- Generates detailed, structured Excel reports with real estate price insights.
- Prevents Windows OS from sleeping during execution to ensure uninterrupted scraping.

### Requirements:
- Python 3.8+
- `pandas`, `requests`, and `xlsxwriter` libraries.
- A `cities.xlsx` file with city and region data.

### Usage:
1. Place the script in the desired directory.
2. Provide the path to the `cities.xlsx` file.
3. Run the script to scrape and process real estate price data, generating an Excel report.

Developed by **Shakhzod**. Initial version released on 9 Dec 2021, with subsequent updates to include OOP and API-based scraping.
