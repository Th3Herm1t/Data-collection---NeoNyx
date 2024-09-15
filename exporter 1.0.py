import sqlite3
import pandas as pd
import os
import json
from tqdm import tqdm

def extract_data_from_sqlite(db_path):
    conn = sqlite3.connect(db_path)
    query = "SELECT * FROM profiles"
    profiles_df = pd.read_sql_query(query, conn)
    conn.close()
    return profiles_df

def read_json_file(profile_id, json_folder_path):
    file_path = os.path.join(json_folder_path, f'{profile_id}_typing.json')
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data

def merge_data(profiles_df, json_folder_path):
    combined_data = []
    for index, row in tqdm(profiles_df.iterrows(), total=len(profiles_df), desc="Processing profiles"):
        profile_id = row['id']
        json_data = read_json_file(profile_id, json_folder_path)
        breakdown_systems = json_data.get('breakdown_systems', {})
        
        profile_entry = {
            'id': profile_id,
            'mbti_profile': row['mbti_profile'],
            'wiki_description': row['wiki_description'],
            'sub_cat_id': row['sub_cat_id'],
            'cat_id': row['cat_id'],
            'property_id': row['property_id'],
            'total_vote_counts': row['total_vote_counts']
        }
        
        for system_id, votes in breakdown_systems.items():
            if votes:  # Check if the list is not empty
                highest_vote = max(votes, key=lambda x: x['theCount'])
                profile_entry[f'system_{system_id}'] = highest_vote['personality_type']
            else:
                profile_entry[f'system_{system_id}'] = ''
        
        combined_data.append(profile_entry)

    combined_df = pd.DataFrame(combined_data)
    return combined_df

def main(db_path, json_folder_path, output_path):
    # Extract and merge data
    profiles_df = extract_data_from_sqlite(db_path)
    combined_df = merge_data(profiles_df, json_folder_path)
    
    # Ensure all system columns are present (up to system_11)
    for i in range(1, 12):
        if f'system_{i}' not in combined_df.columns:
            combined_df[f'system_{i}'] = ''
    
    # Reorder columns
    column_order = ['id', 'mbti_profile', 'wiki_description', 'sub_cat_id', 'cat_id', 'property_id', 'total_vote_counts'] + \
                   [f'system_{i}' for i in range(1, 12)]
    combined_df = combined_df[column_order]
    
    # Save combined data
    combined_df.to_csv(output_path, index=False)
    print(f"Combined data with highest voted personality types saved to {output_path}")

if __name__ == "__main__":
    db_path = 'personality_profiles.db'
    json_folder_path = 'C:/Users/abdel/Desktop/Nyx/data/typing/'
    output_path = 'combined_data_highest_voted.csv'

    main(db_path, json_folder_path, output_path)