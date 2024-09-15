import sqlite3
import requests
import json
import logging
from pathlib import Path
from tqdm import tqdm
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load configuration from file
with open('config.json') as f:
    config = json.load(f)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# API endpoints and other constants
API_URL_PROFILE = "https://api.personality-database.com/api/v1/profile/{}"
API_URL_COMMENTS = "https://api.personality-database.com/api/v1/comments/{}?sort=HOT&offset={}&range=all&version=W3"
TYPING_DATA_DIR = 'data/typing'
COMMENTS_DATA_DIR = 'data/comments'
DB_FILE = 'personality_profiles.db'

# Function to get a proxy from the proxy pool
def get_proxy():
    if not config.get('proxy_enabled', False):
        return None
    if config.get('use_proxy_pool', False):
        proxy_pool_url = config.get('proxy_pool_url')  # Get the proxy URL from the config
        try:
            response = requests.get(proxy_pool_url)
            if response.status_code == 200:
                return response.json().get('proxy')
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch proxy: {str(e)}")
            return None
    return None

# Function to initialize SQLite database and tables
def setup_database():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS profiles (
            id INTEGER PRIMARY KEY,
            mbti_profile TEXT,
            wiki_description TEXT,
            sub_cat_id INTEGER,
            cat_id INTEGER,
            property_id INTEGER,
            total_vote_counts INTEGER
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS errors (
            id INTEGER PRIMARY KEY,
            error_message TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS processed_profiles (
            id INTEGER PRIMARY KEY
        )
    ''')
    conn.commit()
    conn.close()

# Initialize database
setup_database()

def is_profile_processed(profile_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('SELECT id FROM processed_profiles WHERE id = ?', (profile_id,))
    result = c.fetchone()
    conn.close()
    return result is not None

def mark_profile_as_processed(profile_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('INSERT OR REPLACE INTO processed_profiles (id) VALUES (?)', (profile_id,))
    conn.commit()
    conn.close()

def save_error(profile_id, error_message):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('INSERT OR REPLACE INTO errors (id, error_message) VALUES (?, ?)', (profile_id, error_message))
    conn.commit()
    conn.close()

def get_last_processed_id():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('SELECT MAX(id) FROM processed_profiles')
    result = c.fetchone()
    conn.close()
    return result[0] if result[0] is not None else 0

# Function to fetch data from API with proxy support
def fetch_data(profile_id, proxy):
    try:
        proxies = {"http": f"http://{proxy}", "https": f"http://{proxy}"} if proxy else None
        response = requests.get(API_URL_PROFILE.format(profile_id), proxies=proxies)
        if response.status_code == 200:
            return response.json()
        else:
            error_message = f"Failed to fetch data for profile ID {profile_id}: Status code {response.status_code}"
            save_error(profile_id, error_message)
            logging.error(error_message)
            return None
    except requests.exceptions.RequestException as e:
        error_message = f"Request error for profile ID {profile_id}: {str(e)}"
        save_error(profile_id, error_message)
        logging.error(error_message)
        return None

# Function to save detailed typing data to JSON file
def save_typing_data(profile_id, data):
    typing_data = {
        'functions': data.get('functions', []),
        'systems': data.get('systems', []),
        'breakdown_systems': data.get('breakdown_systems', {}),
        'breakdown_config': data.get('breakdown_config', {}),
        'mbti_letter_stats': data.get('mbti_letter_stats', [])
    }
    file_path = Path(TYPING_DATA_DIR) / f'{profile_id}_typing.json'
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, 'w') as f:
        json.dump(typing_data, f, indent=2)

# Function to fetch comments for a profile ID with proxy support
def fetch_comments(profile_id, proxy):
    offset = 0
    comments_data = []
    while True:
        try:
            proxies = {"http": f"http://{proxy}", "https": f"http://{proxy}"} if proxy else None
            response = requests.get(API_URL_COMMENTS.format(profile_id, offset), proxies=proxies)
            if response.status_code == 200:
                data = response.json()
                comments_data.extend(data.get('comments', []))
                offset = data.get('next_offset', 0)
                if not data.get('has_more', False):
                    break
            else:
                logging.error(f"Failed to fetch comments for profile ID {profile_id}: Status code {response.status_code}")
                break
        except requests.exceptions.RequestException as e:
            logging.error(f"Request error for profile ID {profile_id}: {str(e)}")
            break
    
    return comments_data

# Function to save comments to JSON file
def save_comments(profile_id, comments_data):
    file_path = Path(COMMENTS_DATA_DIR) / f'{profile_id}_comments.json'
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, 'w') as f:
        json.dump(comments_data, f, indent=2)

def process_profile(profile_id):
    if is_profile_processed(profile_id):
        logging.info(f"Profile ID {profile_id} is already processed. Skipping.")
        return

    time.sleep(config.get('delay_between_requests', 1.5))  # Configurable delay
    proxy = get_proxy()
    if not proxy and config.get('proxy_enabled', False):
        logging.warning(f"No proxy available for profile ID {profile_id}. Skipping.")
        return

    data = fetch_data(profile_id, proxy)
    if data:
        try:
            profile = {
                'id': data['id'],
                'mbti_profile': data['mbti_profile'],
                'wiki_description': data['wiki_description'],
                'sub_cat_id': data['subcat_link_info']['sub_cat_id'],
                'cat_id': data['subcat_link_info']['cat_id'],
                'property_id': data['subcat_link_info']['property_id'],
                'total_vote_counts': data['total_vote_counts']
            }
            conn = sqlite3.connect(DB_FILE)
            c = conn.cursor()
            c.execute('''
                INSERT OR REPLACE INTO profiles 
                (id, mbti_profile, wiki_description, sub_cat_id, cat_id, property_id, total_vote_counts)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (profile['id'], profile['mbti_profile'], profile['wiki_description'], profile['sub_cat_id'],
                  profile['cat_id'], profile['property_id'], profile['total_vote_counts']))
            conn.commit()
            conn.close()
            save_typing_data(profile_id, data)
            comments_data = fetch_comments(profile_id, proxy)
            save_comments(profile_id, comments_data)
            logging.info(f"Processed profile ID {profile_id}")
            mark_profile_as_processed(profile_id)
        except KeyError as e:
            error_message = f"KeyError processing profile ID {profile_id}: {str(e)}"
            save_error(profile_id, error_message)
            logging.error(error_message)
        except Exception as e:
            error_message = f"Error processing profile ID {profile_id}: {str(e)}"
            save_error(profile_id, error_message)
            logging.error(error_message)
    else:
        logging.warning(f"No data fetched for profile ID {profile_id}")

# Function to fetch all profile IDs to process
def fetch_all_profile_ids(start_id):
    return range(start_id, start_id + config.get('num_profiles_to_scrape', 1000))

# Function to handle fetching and processing in sequential order
def fetch_and_process_profiles(profile_ids):
    max_workers = config.get('max_workers', 10)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_profile, profile_id) for profile_id in profile_ids]
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing profiles"):
            future.result()  # This will raise any exceptions caught during the execution

# Update main function to include comments fetching and processing
def main():
    last_processed_id = get_last_processed_id()
    profile_ids = fetch_all_profile_ids(last_processed_id + 1)
    fetch_and_process_profiles(profile_ids)

if __name__ == "__main__":
    main()
