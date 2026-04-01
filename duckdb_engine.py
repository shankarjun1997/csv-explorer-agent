import duckdb
import pandas as pd
import os

# Initialize a persistent DuckDB connection
db_path = os.path.join(os.path.dirname(__file__), 'data', 'my_db.duckdb')
conn = duckdb.connect(database=db_path, read_only=False)


def init_db():
    """Initialize the database. Drop existing tables to start fresh."""
    try:
        conn.execute("DROP TABLE IF EXISTS csv_data;")
    except Exception as e:
        print(f"Note during init: {e}")


def ingest_csv(file_path, table_name="csv_data"):
    """
    Read a CSV file with Pandas and insert it into DuckDB.
    DuckDB's read_csv is great, but Pandas handles messy data better initially.
    """
    try:
        df = pd.read_csv(file_path)
        
        # Create table if doesn't exist, otherwise append
        if table_exists(table_name):
            # Append to existing table
            conn.execute(f"INSERT INTO {table_name} SELECT * FROM df")
        else:
            # Create new table
            conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
        
        return f"✅ Ingested {len(df)} rows from {os.path.basename(file_path)}"
    except Exception as e:
        return f"❌ Error ingesting {os.path.basename(file_path)}: {str(e)}"


def table_exists(table_name="csv_data"):
    """Check if a table exists in the database."""
    try:
        result = conn.execute(
            f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
        ).fetchall()
        return result[0][0] > 0
    except:
        return False


def get_unique_values(column_name, table_name="csv_data"):
    """Get unique values for a specific column for dropdowns."""
    try:
        result = conn.execute(
            f"SELECT DISTINCT {column_name} FROM {table_name} ORDER BY {column_name} LIMIT 1000"
        ).fetchdf()
        return result[column_name].tolist()
    except Exception as e:
        print(f"Error getting unique values for {column_name}: {e}")
        return []


def get_column_names(table_name="csv_data"):
    """Get list of all column names in the main table."""
    try:
        result = conn.execute(
            f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ordinal_position"
        ).fetchdf()
        return result['column_name'].tolist()
    except Exception as e:
        print(f"Error getting column names: {e}")
        return []


def run_query(sql_query):
    """Execute a custom SQL query and return a DataFrame."""
    try:
        result = conn.execute(sql_query).fetchdf()
        return result, None  # Return result and no error
    except Exception as e:
        return None, str(e)  # Return no result and the error


def get_row_count(table_name="csv_data"):
    """Get the total number of rows in the table."""
    try:
        result = conn.execute(f"SELECT COUNT(*) as count FROM {table_name}").fetchdf()
        return result['count'][0]
    except:
        return 0
