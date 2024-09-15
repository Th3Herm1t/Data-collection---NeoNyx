import sqlite3
import pandas as pd
import os
import json
import tqdm

# Step 1: Extract Data from SQLite Database

def extract_data_from_sqlite(db_path):
    # Connect to SQLite database
    conn = sqlite3.connect(db_path)

    # Query the profiles table
    query = "SELECT * FROM profiles"
    profiles_df = pd.read_sql_query(query, conn)

    # Close the connection
    conn.close()

    return profiles_df

# Step 2: Read JSON Files

def read_json_file(profile_id, json_folder_path):
    file_path = os.path.join(json_folder_path, f'{profile_id}_typing.json')
    with open(file_path, 'r') as file:
        data = json.load(file)
        

    return data

# Step 3: Merge Data

def merge_data(profiles_df, json_folder_path):
    combined_data = []
    n = 1

    # Iterate over profiles and merge JSON data
    for index, row in profiles_df.iterrows():
        
        profile_id = row['id']
        json_data = read_json_file(profile_id, json_folder_path)
        
        # Extract the breakdown_systems data
        breakdown_systems = json_data.get('breakdown_systems', {})
        
        # Iterate through each system in breakdown_systems
        for system_id, votes in breakdown_systems.items():
            for vote in votes:
                combined_entry = {
                    'id': profile_id,
                    'mbti_profile': row['mbti_profile'],
                    'wiki_description': row['wiki_description'],
                    'sub_cat_id': row['sub_cat_id'],
                    'cat_id': row['cat_id'],
                    'property_id': row['property_id'],
                    'total_vote_counts': row['total_vote_counts'],
                    'system_id': system_id,
                    'personality_type': vote['personality_type'],
                    'vote_count': vote['theCount']
                }
                combined_data.append(combined_entry)
        n += 1
        print(f"Processed {n} profiles")

    # Convert combined data into a dataframe
    combined_df = pd.DataFrame(combined_data)
    return combined_df

# Step 4: Save or Use the Combined Dataframe

def save_combined_data(combined_df, output_path):
    # Save to CSV
    combined_df.to_csv(output_path, index=False)

db_path = 'personality_profiles.db'
json_folder_path = 'C:/Users/abdel/Desktop/Nyx/data/typing/'
output_path = 'combined_data.csv'

profiles_df = extract_data_from_sqlite(db_path)
combined_df = merge_data(profiles_df, json_folder_path)
save_combined_data(combined_df, output_path)

# Display the combined dataframe
print(combined_df.head())
