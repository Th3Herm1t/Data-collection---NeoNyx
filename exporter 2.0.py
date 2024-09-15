import os
import json
import pandas as pd
import sqlite3
from tqdm import tqdm

def extract_data_from_sqlite(db_path):
    try:
        conn = sqlite3.connect(db_path)
        query = """
        SELECT id, mbti_profile, sub_cat_id, cat_id, property_id, total_vote_counts
        FROM profiles
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        print(f"Error extracting data from SQLite: {e}")
        return pd.DataFrame()

def read_json_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        return data
    except Exception as e:
        print(f"Error reading JSON file {file_path}: {e}")
        return None

def merge_data(profiles_df, json_folder_path, wiki_folder_path, limit=None):
    combined_data = []

    # If limit is specified, restrict the DataFrame to the first 'limit' rows
    if limit:
        profiles_df = profiles_df.head(limit)

    for index, row in tqdm(profiles_df.iterrows(), total=len(profiles_df), desc="Processing profiles"):
        profile_id = row['id']
        json_data = read_json_file(os.path.join(json_folder_path, f'{profile_id}_typing.json'))

        # Read the Wikipedia data
        wiki_file_path = os.path.join(wiki_folder_path, f'{profile_id}_wiki.json')
        wiki_data = read_json_file(wiki_file_path)

        # Include the entire wiki JSON content in the 'wiki_description' key
        wiki_description = wiki_data if wiki_data else {}
        if not wiki_description:
            print(f"No wiki description found in file: {wiki_file_path}")

        profile_entry = {
            'id': profile_id,
            'mbti_profile': row['mbti_profile'],
            'wiki_description': wiki_description,  # Store entire JSON content here
            'sub_cat_id': row['sub_cat_id'],
            'cat_id': row['cat_id'],
            'property_id': row['property_id'],
            'total_vote_counts': row['total_vote_counts']
        }

        breakdown_systems = json_data.get('breakdown_systems', {})
        for system_id, votes in breakdown_systems.items():
            if votes:
                highest_vote = max(votes, key=lambda x: x['theCount'])
                profile_entry[f'system_{system_id}'] = highest_vote['personality_type']
            else:
                profile_entry[f'system_{system_id}'] = ''
        
        combined_data.append(profile_entry)

    combined_df = pd.DataFrame(combined_data)
    return combined_df

def main(db_path, json_folder_path, wiki_folder_path, output_path, output_format='json', limit=None):
    profiles_df = extract_data_from_sqlite(db_path)
    combined_df = merge_data(profiles_df, json_folder_path, wiki_folder_path, limit)
    
    # Ensure all system columns are present (up to system_11)
    for i in range(1, 12):
        if f'system_{i}' not in combined_df.columns:
            combined_df[f'system_{i}'] = ''
    
    # Reorder columns
    column_order = ['id', 'mbti_profile', 'wiki_description', 'sub_cat_id', 'cat_id', 'property_id', 'total_vote_counts'] + \
                   [f'system_{i}' for i in range(1, 12)]
    combined_df = combined_df[column_order]

    # Save combined data in the chosen format
    if output_format == 'json':
        combined_df.to_json(output_path, orient='records', indent=4)
    elif output_format == 'csv':
        combined_df.to_csv(output_path, index=False)
    print(f"Combined data saved to {output_path} as {output_format.upper()}")

if __name__ == "__main__":
    db_path = 'personality_profiles.db'
    json_folder_path = 'C:/Users/abdel/Desktop/Nyx/data/typing/'
    wiki_folder_path = 'C:/Users/abdel/Desktop/Nyx/data/wiki/'
    output_path = 'combined_data_full_wiki.json'
    output_format = 'json'
    limit = None  # Set your desired limit here

    main(db_path, json_folder_path, wiki_folder_path, output_path, output_format, limit)
