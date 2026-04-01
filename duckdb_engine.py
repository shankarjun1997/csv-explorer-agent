import duckdb
import pandas as pd
import os
import PyPDF2
import pdfplumber
import numpy as np
from scipy import stats

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
    Automatically detects and parses dates.
    """
    try:
        # Read CSV with infer_datetime_format
        df = pd.read_csv(file_path, infer_datetime_format=True, parse_dates=True)
        
        # Convert datetime columns to string for DuckDB compatibility
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].astype(str)
        
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


def ingest_pdf(file_path, table_name="csv_data"):
    """Extract tabular data from PDF and ingest into DuckDB."""
    try:
        dfs = []
        
        # Try pdfplumber first (better for tables)
        try:
            with pdfplumber.open(file_path) as pdf:
                for page in pdf.pages:
                    tables = page.extract_tables()
                    if tables:
                        for table in tables:
                            df_table = pd.DataFrame(table[1:], columns=table[0])
                            dfs.append(df_table)
        except Exception as e:
            print(f"pdfplumber error: {e}")
        
        if not dfs:
            return f"❌ No tables found in {os.path.basename(file_path)}"
        
        df = pd.concat(dfs, ignore_index=True)
        
        # Create table if doesn't exist
        if table_exists(table_name):
            conn.execute(f"INSERT INTO {table_name} SELECT * FROM df")
        else:
            conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
        
        return f"✅ Extracted {len(df)} rows from PDF {os.path.basename(file_path)}"
    except Exception as e:
        return f"❌ Error processing PDF: {str(e)}"


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


def get_column_types(table_name="csv_data"):
    """Get column names and their data types."""
    try:
        result = conn.execute(
            f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ordinal_position"
        ).fetchdf()
        return dict(zip(result['column_name'], result['data_type']))
    except Exception as e:
        print(f"Error getting column types: {e}")
        return {}


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


def detect_outliers(column_name, method="iqr", table_name="csv_data"):
    """
    Detect outliers using IQR or Z-score method.
    Returns dataframe with outliers marked.
    """
    try:
        # Get the data
        result_df, _ = run_query(f"SELECT * FROM {table_name}")
        
        # Check if column is numeric
        if column_name not in result_df.columns:
            return None, f"Column '{column_name}' not found"
        
        data = pd.to_numeric(result_df[column_name], errors='coerce')
        
        if method == "iqr":
            Q1 = data.quantile(0.25)
            Q3 = data.quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            outliers = (data < lower_bound) | (data > upper_bound)
        else:  # z-score
            z_scores = np.abs(stats.zscore(data.dropna()))
            threshold = 3
            outlier_indices = z_scores > threshold
            outliers = pd.Series(False, index=data.index)
            outliers[data.dropna().index[outlier_indices]] = True
        
        result_df['is_outlier'] = outliers
        return result_df, None
    except Exception as e:
        return None, str(e)


def get_numeric_columns(table_name="csv_data"):
    """Get list of numeric columns for charting."""
    try:
        col_types = get_column_types(table_name)
        numeric_cols = [col for col, dtype in col_types.items() 
                       if 'VARCHAR' not in dtype and 'DATE' not in dtype and 'TIMESTAMP' not in dtype]
        return numeric_cols
    except:
        return []


def get_date_columns(table_name="csv_data"):
    """Get list of date columns."""
    try:
        col_types = get_column_types(table_name)
        date_cols = [col for col, dtype in col_types.items() 
                    if 'DATE' in dtype or 'TIMESTAMP' in dtype]
        return date_cols
    except:
        return []

