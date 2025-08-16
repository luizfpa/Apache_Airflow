import sqlite3
import logging
import pandas as pd
import random
import numpy as np
from airflow.models import Variable

def get_table_schema(db_path, table_name):
    """
    Retrieves the schema of a table from a SQLite database.
    
    Args:
        db_path (str): Path to the SQLite database.
        table_name (str): Name of the table.
    
    Returns:
        list: List of column names in the table.
    """
    logging.info(f"Retrieving schema for table {table_name} in database {db_path}")
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [info[1] for info in cursor.fetchall()]
        conn.close()
        logging.info(f"Schema for {table_name}: {columns}")
        return columns
    except Exception as e:
        logging.error(f"Error retrieving schema for {table_name}: {str(e)}")
        raise

def extract_data(db_path, query):
    """
    Extract data from a SQLite database based on an SQL query.
    
    Args:
        db_path (str): Path to the SQLite database.
        query (str): SQL query for extraction.
    
    Returns:
        pandas.DataFrame: Extracted data.
    """
    logging.info(f"Starting data extraction from database {db_path} with query: {query}")
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query(query, conn)
        conn.close()
        logging.info(f"Extraction completed. {len(df)} rows extracted.")
        return df
    except Exception as e:
        logging.error(f"Error during extraction: {str(e)}")
        raise

def transform_data(data, transformation_type, *,db_path=None, table_name=None):
    """
    Transforms the extracted data based on the type of transformation.
    
    Args:
        data (pandas.DataFrame): Data to transform.
        transformation_type (str): Type of transformation ('aggregate', 'clean', etc.).
        db_path (str, optional): Path to the SQLite database for schema validation.
        table_name (str, optional): Name of the table for schema validation.
    
    Returns:
        pandas.DataFrame: Transformed data.
    """
    logging.info(f"Starting transformation of type: {transformation_type}")
    try:
        if transformation_type == 'aggregate':
            # Check schema if db_path and table_name are provided
            category_column = 'category'
            amount_column = 'amount'
            if db_path and table_name:
                columns = get_table_schema(db_path, table_name)
                if 'category' not in columns:
                    # Fallback to alternative column (e.g., product_name for inventory)
                    if 'product_name' in columns:
                        category_column = 'product_name'
                        logging.info(f"Using 'product_name' as category column for {table_name}")
                    else:
                        raise ValueError(f"No suitable category column found in {table_name}. Available columns: {columns}")
                if 'amount' not in columns and 'quantity' in columns:
                    amount_column = 'quantity'
                    logging.info(f"Using 'quantity' as amount column for {table_name}")
            # Aggregate values by category
            df = data.groupby(category_column).agg({amount_column: 'sum'}).reset_index()
            df.columns = ['category', 'amount']  # Standardize output columns
        elif transformation_type == 'clean':
            # For visit_logs, use 'page_url' as category and create dummy amount
            df = data.dropna()
            df = data.dropna()
            if 'page_url' in df.columns:
                df = df.rename(columns={'page_url': 'category'})
                df['amount'] = np.random.randint(1, 101, size=len(df))
                df = df[['category', 'amount']]  # Select only required columns
            elif 'name' in df.columns:
                df['name'] = df['name'].str.lower().str.strip()
                df = df.rename(columns={'name': 'category'})
                df['amount'] = np.random.randint(1, 101, size=len(df))
                df = df[['category', 'amount']]
            else:
                df = data[['category', 'amount']]  # Fallback if no specific column
        else:
            df = data  # No transformation, return original
        logging.info(f"Transformation completed. {len(df)} rows after transformation.")
        return df
    except Exception as e:
        logging.error(f"Error during transformation: {str(e)}")
        raise

def load_data(db_path, data, table):
    """
    Loads data into a table in the SQLite database.
    
    Args:
        db_path (str): Path to the SQLite database.
        data (pandas.DataFrame): Data to load.
        table (str): Name of the destination table.
    """
    logging.info(f"Starting to load {len(data)} rows into table {table} of database {db_path}")
    try:
        conn = sqlite3.connect(db_path)
        data.to_sql(table, conn, if_exists='append', index=False)
        conn.close()
        logging.info("Load completed successfully.")
    except Exception as e:
        logging.error(f"Error during load: {str(e)}")
        raise