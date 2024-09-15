import wikipediaapi
import json
import time
import logging
import sqlite3
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define a detailed custom user agent
USER_AGENT = 'DataScience-WikipediaBot/1.0 (th3hermit@protonmail.com)'

# Initialize Wikipedia API with the custom user agent
wiki_wiki = wikipediaapi.Wikipedia(user_agent=USER_AGENT)

# Ensure the output directory exists
output_dir = 'data/wiki'
os.makedirs(output_dir, exist_ok=True)

def fetch_wikipedia_content(page_name):
    try:
        page = wiki_wiki.page(page_name)
        if not page.exists():
            error_message = f"Page '{page_name}' does not exist."
            logging.warning(error_message)
            return None, error_message
        return page, None
    except Exception as e:
        error_message = f"Error fetching page '{page_name}': {str(e)}"
        logging.error(error_message)
        return None, error_message

def extract_section_text(section):
    section_text = section.text
    for subsection in section.sections:
        section_text += extract_section_text(subsection)
    return section_text

def extract_info(page):
    try:
        data = {
            'title': page.title,
            'url': page.fullurl,
            'summary': page.summary,
            'sections': {},
            'categories': list(page.categories.keys())
        }
        for section in page.sections:
            data['sections'][section.title] = extract_section_text(section)
        return data, None
    except Exception as e:
        error_message = f"Error extracting information from page '{page.title}': {str(e)}"
        logging.error(error_message)
        return None, error_message

def save_to_json(data, filename):
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logging.info("Data saved to %s", filename)
    except Exception as e:
        logging.error("Error saving data to JSON: %s", e)

def save_progress(celeb_id):
    try:
        conn = sqlite3.connect('personality_profiles.db')
        cursor = conn.cursor()
        cursor.execute("INSERT OR REPLACE INTO wiki_progress (id, last_processed_id) VALUES (1, ?)", (celeb_id,))
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error("Error saving progress: %s", e)

def load_progress():
    try:
        conn = sqlite3.connect('personality_profiles.db')
        cursor = conn.cursor()
        cursor.execute("SELECT last_processed_id FROM wiki_progress WHERE id = 1")
        row = cursor.fetchone()
        conn.close()
        if row:
            return row[0]
    except Exception as e:
        logging.error("Error loading progress: %s", e)
    return None

def save_error(celeb_id, celeb_name, error_message):
    try:
        conn = sqlite3.connect('personality_profiles.db')
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO wiki_errors (celeb_id, celeb_name, error_message)
            VALUES (?, ?, ?)
        """, (celeb_id, celeb_name, error_message))
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error("Error saving to wiki_errors table: %s", e)

def create_tables():
    try:
        conn = sqlite3.connect('personality_profiles.db')
        cursor = conn.cursor()
        # Create the progress table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS wiki_progress (
                id INTEGER PRIMARY KEY,
                last_processed_id INTEGER
            )
        """)
        # Create the errors table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS wiki_errors (
                error_id INTEGER PRIMARY KEY AUTOINCREMENT,
                celeb_id INTEGER,
                celeb_name TEXT,
                error_message TEXT,
                error_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error("Error creating tables: %s", e)

def process_celebrity(celeb_id, celeb_name):
    logging.info("Fetching data for %s (ID: %d)...", celeb_name, celeb_id)
    page, error_message = fetch_wikipedia_content(celeb_name)
    if page:
        info, extract_error = extract_info(page)
        if info:
            filename = os.path.join(output_dir, f"{celeb_id}_wiki.json")
            save_to_json(info, filename)
            save_progress(celeb_id)
        elif extract_error:
            save_error(celeb_id, celeb_name, extract_error)
    elif error_message:
        save_error(celeb_id, celeb_name, error_message)

def main():
    create_tables()

    conn = sqlite3.connect('personality_profiles.db')
    cursor = conn.cursor()

    # Retrieve the list of celebrities (property_id 1 for public figures and 2 for fictional characters)
    cursor.execute("SELECT id, mbti_profile FROM profiles WHERE property_id IN (1, 2)")
    celebrities = cursor.fetchall()
    conn.close()

    # Load the last processed ID
    last_processed_id = load_progress()
    if last_processed_id is not None:
        # Skip the celebrities that have been processed
        celebrities = [celeb for celeb in celebrities if celeb[0] > last_processed_id]

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_to_celeb = {executor.submit(process_celebrity, celeb_id, celeb_name): (celeb_id, celeb_name) for celeb_id, celeb_name in celebrities}
            for future in as_completed(future_to_celeb):
                celeb_id, celeb_name = future_to_celeb[future]
                try:
                    future.result()
                except Exception as e:
                    logging.error("Error processing %s (ID: %d): %s", celeb_name, celeb_id, e)
                time.sleep(1)  # Sleep to avoid hitting Wikipedia's rate limits
    except KeyboardInterrupt:
        logging.info("Process interrupted. Saving progress and exiting.")
    finally:
        pass

if __name__ == "__main__":
    main()
