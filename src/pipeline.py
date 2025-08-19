import psycopg2
import pandas as pd
from psycopg2 import sql

# --- Database Configuration ---
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASS = "techlab"
DB_HOST = "localhost"
DB_PORT = "5432"

# --- File and Table Configuration ---
CSV_FILE_PATH = 'data/patients.csv'
TABLE_NAME = 'patients'

def get_db_connection():
    """Establishes and returns a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=DB_PORT
        )
        print("Database connection established successfully.")
        return conn
    except psycopg2.OperationalError as e:
        print(f"Could not connect to the database: {e}")
        return None

def create_table_from_dataframe(conn, df, table_name):
    """Creates a table in the database based on the DataFrame structure."""
    # Drop table if it exists to ensure a fresh start
    with conn.cursor() as cur:
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(table_name)))
        print(f"Dropped existing table '{table_name}'.")

    # Create the table creation SQL statement
    # This is a simplified type mapping; for production, you might need more robust type inference
    cols = []
    for col_name, dtype in df.dtypes.items():
        if "int" in str(dtype):
            sql_type = "INTEGER"
        elif "float" in str(dtype):
            sql_type = "REAL"
        else:
            sql_type = "TEXT"
        cols.append(f'"{col_name}" {sql_type}')
    
    create_table_query = f'CREATE TABLE "{table_name}" ({", ".join(cols)});'

    with conn.cursor() as cur:
        cur.execute(create_table_query)
        conn.commit()
        print(f"Table '{table_name}' created successfully.")

def insert_data_from_dataframe(conn, df, table_name):
    """Inserts data from a pandas DataFrame into the specified table."""
    # Prepare the data for insertion (list of tuples)
    data_to_insert = [tuple(x) for x in df.to_numpy()]
    
    # Create the insert statement
    cols = '", "'.join([str(i) for i in df.columns.tolist()])
    insert_query = f'INSERT INTO "{table_name}" ("{cols}") VALUES %s'

    with conn.cursor() as cur:
        from psycopg2.extras import execute_values
        execute_values(cur, insert_query, data_to_insert)
        conn.commit()
        print(f"{len(data_to_insert)} rows inserted into '{table_name}' successfully.")

def main():
    """Main function to run the data pipeline."""
    print("Starting the data pipeline...")
    
    # 1. Connect to the database
    conn = get_db_connection()
    if not conn:
        return

    try:
        # 2. Read the CSV file
        try:
            df = pd.read_csv(CSV_FILE_PATH)
            print(f"Successfully loaded '{CSV_FILE_PATH}' into a DataFrame.")
        except FileNotFoundError:
            print(f"Error: The file '{CSV_FILE_PATH}' was not found.")
            return

        # 3. Create the table
        create_table_from_dataframe(conn, df, TABLE_NAME)

        # 4. Insert the data
        insert_data_from_dataframe(conn, df, TABLE_NAME)

    except Exception as e:
        print(f"An error occurred during the pipeline execution: {e}")
    finally:
        # 5. Close the connection
        if conn:
            conn.close()
            print("Database connection closed.")
    
    print("Data pipeline finished successfully.")

if __name__ == '__main__':
    main()
