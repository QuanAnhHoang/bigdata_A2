# About load dataset from CSV file into MongoDB Cluster
# Step 1: Open database_service.cfg file
# Step 2: Change your mongoDB config with username, password and cluster_url
# Step 3: If you want to add your data, throw it in Data folder
# Step 4: Open database.py
# Step 5: Run and wait for it until complete data uploaded (you can have a cup of tea while waiting =]]] )

import pandas as pd
import os
import pymongo
import configparser

config = configparser.ConfigParser()
config.read('database_service.cfg')
api_credential = config['mongodb_information_credential']

database_username = api_credential['mongodb_username']
database_password = api_credential['mongodb_password']
database_cluster_name = api_credential['mongodb_cluster_url']

connection_string = f"mongodb+srv://{database_username}:{database_password}@{database_cluster_name}/?retryWrites=true&w=majority"

directory_path = f"{os.path.dirname(os.path.realpath(__file__))}/Data"



def read_csv_files_in_folder_recursive(folder_path):
    dataframes_by_category_and_year = {}

    # Recursive function to traverse subdirectories
    def traverse_directory(current_folder):
        for filename in os.listdir(current_folder):
            file_path = os.path.join(current_folder, filename)

            if os.path.isdir(file_path):
                # Recursively traverse subdirectories
                traverse_directory(file_path)
            elif filename.endswith(".csv"):
                # Extract category and year from the folder and filename
                category = os.path.basename(current_folder)
                year = int(filename.split('_')[-1].split('.')[0])

                # Read the CSV file into a DataFrame
                df = pd.read_csv(file_path)

                # Add a new "year" column to the DataFrame
                df['year'] = year

                # Add the DataFrame to the dictionary using category and year as keys
                key = (category, year)
                if key in dataframes_by_category_and_year:
                    dataframes_by_category_and_year[key].append(df)
                else:
                    dataframes_by_category_and_year[key] = [df]

    traverse_directory(folder_path)

    # Concatenate DataFrames for each category and year
    for key, dfs in dataframes_by_category_and_year.items():
        dataframes_by_category_and_year[key] = pd.concat(dfs, ignore_index=True)

    return dataframes_by_category_and_year


def insert_dataframe_into_mongodb(mongo_url, database_name, dataframes_by_category_and_year):
    try:
        # Connect to MongoDB
        client = pymongo.MongoClient(mongo_url)

        # Check if the connection was successful
        if client.server_info():
            print("Connected to MongoDB successfully!")

        
        db = client[database_name]

        for key, df in dataframes_by_category_and_year.items():
        
            category, year = key
            collection_name = f"data_{category}_{year}"
            collection = db[collection_name]

            # Convert dataframe to dictionary
            data_dict = df.to_dict(orient='records')

            # Insert data into MongoDB
            collection.insert_many(data_dict)


            print(f"Successfully added data for {category} in {year}")

        client.close()

        print("Successfully added all data")

    except Exception as err:
        print(f"Mongo connection error: {err}")
  
def check_null_values(dataframes_by_category_and_year):
    for key, dataframe in dataframes_by_category_and_year.items():
        category, year = key
        print(f"Checking null values for {category} - {year}")
        print(dataframe.isnull().sum())

    
if __name__ == "__main__":

    dataframe_grouped = read_csv_files_in_folder_recursive(directory_path)
    check_null_values(dataframe_grouped)

    successfulDataInserted = insert_dataframe_into_mongodb(
        mongo_url=connection_string,
        database_name="Dutch_energy",
        dataframes_by_category_and_year=dataframe_grouped
    )