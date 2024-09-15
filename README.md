# Personality Profiles Data Scraper

This project is a multi-threaded data scraper that collects and processes personality profile data from an API and Wikipedia. It includes SQLite database handling, JSON data storage, and support for proxy usage. The scraper processes personality profiles, typing data, and comments from the Personality Database API, and enriches the data with detailed Wikipedia information.

## Table of Contents
- [Features](#features)
- [Project Structure](#project-structure)
- [Requirements](#requirements)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Database Schema](#database-schema)
- [Logging](#logging)
- [Error Handling](#error-handling)
- [Contributing](#contributing)
- [License](#license)

## Features
- **Personality Data Scraping**: Fetches personality profile data from the Personality Database API.
- **Comments Scraping**: Retrieves user comments associated with profiles.
- **Typing Data Processing**: Extracts and saves typing breakdowns and systems into structured JSON files.
- **Wikipedia Enrichment**: Fetches related Wikipedia pages to enrich the profile data.
- **SQLite Integration**: Saves profile information, errors, and processing status in an SQLite database.
- **Multi-threading**: Uses `ThreadPoolExecutor` for concurrent scraping and processing.
- **Proxy Support**: Optionally supports scraping through a proxy pool.
  
## Project Structure

```
.
├── data/
│   ├── typing/                # Directory to store typing data JSON files
│   ├── comments/              # Directory to store comments data JSON files
│   ├── wiki/                  # Directory to store Wikipedia data JSON files
├── personality_profiles.db     # SQLite database to store profile data
├── config.json                 # Configuration file for scraper settings
├── main.py                     # Main script for scraping personality data
├── wikipedia.py             # Script to scrape and enrich data with Wikipedia information
├── exporter 2.0.py              # Script to merge personality and Wikipedia data
└── README.md                   # Project documentation
```

## Requirements

- Python 3.7+
- SQLite3
- `requests` - HTTP library
- `tqdm` - Progress bar
- `wikipediaapi` - API to interact with Wikipedia
- `concurrent.futures` - For threading
- `pandas` - For data merging and processing
- `logging` - For logging events
- `json` - For handling JSON data

## Installation

1. Clone the repository:

    ```bash
    git clone https://github.com/yourusername/personality-profiles-scraper.git
    cd personality-profiles-scraper
    ```

2. Create a virtual environment and activate it:

    ```bash
    python3 -m venv venv
    source venv/bin/activate   # On Windows: venv\Scripts\activate
    ```

3. Install the required dependencies:

    ```bash
    pip install -r requirements.txt
    ```

## Configuration

The scraper is configured using the `config.json` file. Update the configuration based on your requirements:

```json
{
    "proxy_enabled": false,
    "max_workers": 10,
    "delay_between_requests": 1.5,
    "num_profiles_to_scrape": 1000,
    "use_proxy_pool": false,
    "proxy_pool_url": "http://localhost:5010/get?type=https"
}
```

### Key Configurations

- `proxy_enabled`: Enables or disables proxy usage.
- `max_workers`: Number of threads to use for concurrent processing.
- `delay_between_requests`: Delay between API requests to avoid rate limiting.
- `num_profiles_to_scrape`: Number of profiles to scrape in each batch.
- `use_proxy_pool`: Enables fetching proxies from a proxy pool.
- `proxy_pool_url`: URL to the proxy pool service.

## Usage

### Running the Personality Profiles Scraper

To scrape personality profiles from the Personality Database API:

```bash
python main.py
```

This script will:

- Fetch personality profile data.
- Save typing data and user comments into JSON files.
- Store processed profiles in the SQLite database to prevent reprocessing.

### Running the Wikipedia Enrichment Script

To scrape Wikipedia pages related to the personality profiles:

```bash
python wikipedia.py
```

This script will:

- Fetch Wikipedia pages for public figures and fictional characters.
- Extract and save page summaries, sections, and categories into JSON files.
- Log errors and maintain scraping progress in the SQLite database.

### Merging Data

To merge the profile data, typing data, and Wikipedia information into a single dataset:

```bash
python exporter 2.0.py
```

This script will:

- Extract data from the SQLite database.
- Read related JSON files (typing and Wikipedia data).
- Combine everything into a structured format and save it as a JSON or CSV file.

### Command-Line Arguments for `data_merger.py`

- `db_path`: Path to the SQLite database.
- `json_folder_path`: Directory containing typing data JSON files.
- `wiki_folder_path`: Directory containing Wikipedia JSON files.
- `output_path`: Path to save the combined output.
- `output_format`: Format of the output file (`json` or `csv`).

Example:

```bash
python exporter 2.0.py --output_format json
```

## Database Schema

The SQLite database consists of several tables:

1. **profiles**: Stores scraped profile data (e.g., MBTI profile, category, vote counts).
   
   ```sql
   CREATE TABLE profiles (
       id INTEGER PRIMARY KEY,
       mbti_profile TEXT,
       wiki_description TEXT,
       sub_cat_id INTEGER,
       cat_id INTEGER,
       property_id INTEGER,
       total_vote_counts INTEGER
   );
   ```

2. **processed_profiles**: Tracks which profiles have been processed.
   
   ```sql
   CREATE TABLE processed_profiles (
       id INTEGER PRIMARY KEY
   );
   ```

3. **errors**: Stores errors encountered during processing.

   ```sql
   CREATE TABLE errors (
       id INTEGER PRIMARY KEY,
       error_message TEXT
   );
   ```

4. **wiki_progress**: Tracks the last processed Wikipedia page for resuming the scraping process.

   ```sql
   CREATE TABLE wiki_progress (
       id INTEGER PRIMARY KEY,
       last_processed_id INTEGER
   );
   ```

## Logging

The scraper logs all events, including data fetching, processing, and errors. Logs can be found in the console and follow the format:

```
2023-01-01 12:00:00 - INFO - Processed profile ID 123
2023-01-01 12:01:00 - ERROR - Request error for profile ID 124: Connection timed out
```

## Error Handling

Errors are stored in the `errors` table in the SQLite database, along with relevant details such as the profile ID and error message. Wikipedia errors are logged in the `wiki_errors` table.

## Contributing

Contributions to improve the project are welcome! Please fork the repository, create a new branch for your feature or bugfix, and submit a pull request.

1. Fork the project.
2. Create a feature branch (`git checkout -b feature-new`).
3. Commit your changes (`git commit -am 'Add a new feature'`).
4. Push to the branch (`git push origin feature-new`).
5. Open a pull request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.
