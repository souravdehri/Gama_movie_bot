import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection settings
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# Function to check the connection
def check_database_connection():
    try:
        # Attempt to connect to the database
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        print("Database connection successful!")
        conn.close()
        return True
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return False

# Function to download table data as CSV
def download_table_as_csv(table_name, output_file="output.csv"):
    if not check_database_connection():
        print("Unable to download CSV. Database connection failed.")
        return
    
    try:
        # Connect to the database
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )

        # Query to get data from the specified table
        query = f"SELECT * FROM {table_name};"
        df = pd.read_sql_query(query, conn)

        # Save the data to a CSV file
        df.to_csv(output_file, index=False)
        print(f"CSV file '{output_file}' has been created successfully!")

        conn.close()
    except Exception as e:
        print(f"Error downloading the table as CSV: {e}")

# Main function to run the checks
if __name__ == "__main__":
    table_name = input("Enter the table name you want to export to CSV: ")
    download_table_as_csv(table_name)
