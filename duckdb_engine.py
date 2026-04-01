"""DuckDB database engine and operations."""

import duckdb
import pandas as pd
import pdfplumber
import numpy as np
from scipy import stats
from typing import Tuple, Optional, List, Dict, Any
from config import DATABASE_PATH, DATABASE_READ_ONLY, OUTLIER_IQR_MULTIPLIER, OUTLIER_ZSCORE_THRESHOLD
from logger import setup_logger

logger = setup_logger(__name__)


class DatabaseEngine:
    """Database engine for DuckDB operations."""
    
    def __init__(self, db_path: str = DATABASE_PATH):
        """Initialize database connection."""
        self.db_path = db_path
        self.conn = None
        self._connect()
    
    def _connect(self):
        """Establish database connection."""
        try:
            self.conn = duckdb.connect(database=self.db_path, read_only=DATABASE_READ_ONLY)
            logger.info(f"Connected to database: {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def init_db(self):
        """Initialize database by dropping existing table."""
        try:
            self.conn.execute("DROP TABLE IF EXISTS csv_data;")
            logger.info("Database initialized")
        except Exception as e:
            logger.warning(f"Error during init: {e}")
    
    def ingest_csv(self, file_path: str, table_name: str = "csv_data") -> str:
        """Ingest CSV file with proper date parsing."""
        try:
            df = pd.read_csv(file_path, infer_datetime_format=True, parse_dates=True)
            
            # Convert datetime columns to string for DuckDB compatibility
            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = df[col].astype(str)
            
            # Create or append to table
            if self.table_exists(table_name):
                self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM df")
            else:
                self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
            
            rows_count = len(df)
            logger.info(f"Ingested {rows_count} rows from {file_path}")
            return f"✅ Ingested {rows_count:,} rows"
            
        except Exception as e:
            error_msg = f"Error ingesting CSV: {str(e)}"
            logger.error(error_msg)
            return f"❌ {error_msg}"
    
    def ingest_pdf(self, file_path: str, table_name: str = "csv_data") -> str:
        """Extract tables from PDF and ingest into database."""
        try:
            dfs = []
            
            with pdfplumber.open(file_path) as pdf:
                for page_num, page in enumerate(pdf.pages):
                    tables = page.extract_tables()
                    if tables:
                        for table in tables:
                            if len(table) > 1:
                                df_table = pd.DataFrame(table[1:], columns=table[0])
                                dfs.append(df_table)
            
            if not dfs:
                return f"❌ No tables found in PDF"
            
            df = pd.concat(dfs, ignore_index=True)
            
            # Create or append table
            if self.table_exists(table_name):
                self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM df")
            else:
                self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
            
            rows_count = len(df)
            logger.info(f"Extracted {rows_count} rows from PDF: {file_path}")
            return f"✅ Extracted {rows_count:,} rows"
            
        except Exception as e:
            error_msg = f"Error processing PDF: {str(e)}"
            logger.error(error_msg)
            return f"❌ {error_msg}"
    
    def table_exists(self, table_name: str = "csv_data") -> bool:
        """Check if table exists in database."""
        try:
            result = self.conn.execute(
                f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
            ).fetchall()
            return result[0][0] > 0
        except Exception as e:
            logger.warning(f"Error checking table existence: {e}")
            return False
    
    def get_column_names(self, table_name: str = "csv_data") -> List[str]:
        """Get all column names."""
        try:
            result = self.conn.execute(
                f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ordinal_position"
            ).fetchdf()
            return result['column_name'].tolist()
        except Exception as e:
            logger.error(f"Error getting column names: {e}")
            return []
    
    def get_column_types(self, table_name: str = "csv_data") -> Dict[str, str]:
        """Get column names and their data types."""
        try:
            result = self.conn.execute(
                f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ordinal_position"
            ).fetchdf()
            return dict(zip(result['column_name'], result['data_type']))
        except Exception as e:
            logger.error(f"Error getting column types: {e}")
            return {}
    
    def get_unique_values(self, column_name: str, table_name: str = "csv_data", limit: int = 1000) -> List[Any]:
        """Get unique values for a column."""
        try:
            result = self.conn.execute(
                f"SELECT DISTINCT {column_name} FROM {table_name} ORDER BY {column_name} LIMIT {limit}"
            ).fetchdf()
            return result[column_name].tolist()
        except Exception as e:
            logger.warning(f"Error getting unique values for {column_name}: {e}")
            return []
    
    def get_row_count(self, table_name: str = "csv_data") -> int:
        """Get total row count."""
        try:
            result = self.conn.execute(f"SELECT COUNT(*) as count FROM {table_name}").fetchdf()
            return result['count'][0]
        except Exception as e:
            logger.error(f"Error getting row count: {e}")
            return 0
    
    def run_query(self, sql_query: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """Execute SQL query and return results."""
        try:
            result = self.conn.execute(sql_query).fetchdf()
            logger.info(f"Query executed successfully, returned {len(result)} rows")
            return result, None
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Query execution failed: {error_msg}")
            return None, error_msg
    
    def get_numeric_columns(self, table_name: str = "csv_data") -> List[str]:
        """Get list of numeric columns."""
        try:
            col_types = self.get_column_types(table_name)
            return [col for col, dtype in col_types.items() 
                   if col_type_is_numeric(dtype)]
        except Exception as e:
            logger.error(f"Error getting numeric columns: {e}")
            return []
    
    def get_date_columns(self, table_name: str = "csv_data") -> List[str]:
        """Get list of date columns."""
        try:
            col_types = self.get_column_types(table_name)
            return [col for col, dtype in col_types.items() 
                   if 'DATE' in dtype or 'TIMESTAMP' in dtype]
        except Exception as e:
            logger.error(f"Error getting date columns: {e}")
            return []
    
    def detect_outliers(self, column_name: str, method: str = "iqr", table_name: str = "csv_data") -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """Detect outliers using IQR or Z-score method."""
        try:
            result_df, error = self.run_query(f"SELECT * FROM {table_name}")
            
            if error or result_df is None:
                return None, error
            
            if column_name not in result_df.columns:
                return None, f"Column '{column_name}' not found"
            
            data = pd.to_numeric(result_df[column_name], errors='coerce')
            
            if method == "iqr":
                Q1 = data.quantile(0.25)
                Q3 = data.quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - OUTLIER_IQR_MULTIPLIER * IQR
                upper_bound = Q3 + OUTLIER_IQR_MULTIPLIER * IQR
                outliers = (data < lower_bound) | (data > upper_bound)
            else:  # z-score
                z_scores = np.abs(stats.zscore(data.dropna()))
                outlier_indices = z_scores > OUTLIER_ZSCORE_THRESHOLD
                outliers = pd.Series(False, index=data.index)
                outliers[data.dropna().index[outlier_indices]] = True
            
            result_df['is_outlier'] = outliers
            logger.info(f"Detected {outliers.sum()} outliers in column {column_name} using {method.upper()}")
            return result_df, None
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error detecting outliers: {error_msg}")
            return None, error_msg
    
    def get_column_stats(self, column_name: str, table_name: str = "csv_data") -> Dict[str, Any]:
        """Get statistical summary for a column."""
        try:
            result_df, _ = self.run_query(f"SELECT {column_name} FROM {table_name}")
            
            if result_df is None:
                return {}
            
            series = result_df[column_name]
            
            stats_dict = {
                'type': str(series.dtype),
                'non_null': series.notna().sum(),
                'null': series.isna().sum(),
                'unique': series.nunique(),
            }
            
            if pd.api.types.is_numeric_dtype(series):
                numeric_series = pd.to_numeric(series, errors='coerce')
                stats_dict.update({
                    'mean': numeric_series.mean(),
                    'median': numeric_series.median(),
                    'std': numeric_series.std(),
                    'min': numeric_series.min(),
                    'max': numeric_series.max(),
                    'q25': numeric_series.quantile(0.25),
                    'q75': numeric_series.quantile(0.75),
                })
            
            return stats_dict
        except Exception as e:
            logger.error(f"Error getting column stats: {e}")
            return {}


def col_type_is_numeric(col_type: str) -> bool:
    """Check if column type is numeric."""
    numeric_types = ['INTEGER', 'BIGINT', 'INT', 'DOUBLE', 'FLOAT', 'DECIMAL', 'NUMERIC']
    return any(num_type in col_type.upper() for num_type in numeric_types)


# Global database instance
db = DatabaseEngine()

