"""Utility functions."""

import pandas as pd
import numpy as np
from typing import List, Dict, Tuple, Any
import streamlit as st
from logger import setup_logger

logger = setup_logger(__name__)


@st.cache_data
def format_number(num: float, decimals: int = 2) -> str:
    """Format a number with thousand separators."""
    try:
        return f"{float(num):,.{decimals}f}"
    except (ValueError, TypeError):
        return str(num)


@st.cache_data
def format_file_size(size_bytes: int) -> str:
    """Format bytes to human-readable size."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} TB"


def safe_cast_to_numeric(series: pd.Series) -> pd.Series:
    """Safely cast series to numeric, handling errors."""
    try:
        return pd.to_numeric(series, errors='coerce')
    except Exception as e:
        logger.warning(f"Error casting to numeric: {e}")
        return series


def get_data_quality_metrics(df: pd.DataFrame) -> Dict[str, Any]:
    """Calculate data quality metrics."""
    try:
        total_cells = df.shape[0] * df.shape[1]
        missing_cells = df.isnull().sum().sum()
        completeness = ((total_cells - missing_cells) / total_cells * 100) if total_cells > 0 else 0
        
        duplicate_rows = df.duplicated().sum()
        
        return {
            'total_rows': df.shape[0],
            'total_columns': df.shape[1],
            'total_cells': total_cells,
            'missing_cells': missing_cells,
            'completeness': completeness,
            'duplicate_rows': duplicate_rows,
            'duplicate_percentage': (duplicate_rows / df.shape[0] * 100) if df.shape[0] > 0 else 0
        }
    except Exception as e:
        logger.error(f"Error calculating quality metrics: {e}")
        return {}


def get_column_summary(series: pd.Series) -> Dict[str, Any]:
    """Get summary statistics for a column."""
    try:
        summary = {
            'type': str(series.dtype),
            'non_null': series.notna().sum(),
            'null': series.isna().sum(),
            'unique': series.nunique(),
        }
        
        if pd.api.types.is_numeric_dtype(series):
            numeric_series = pd.to_numeric(series, errors='coerce')
            summary.update({
                'mean': numeric_series.mean(),
                'median': numeric_series.median(),
                'std': numeric_series.std(),
                'min': numeric_series.min(),
                'max': numeric_series.max(),
            })
        
        return summary
    except Exception as e:
        logger.error(f"Error getting column summary: {e}")
        return {}


def build_where_clause(filters: Dict[str, List[Any]]) -> Tuple[str, List[str]]:
    """Build SQL WHERE clause from filters."""
    where_clauses = []
    
    try:
        for col, values in filters.items():
            if values:
                # Handle different data types
                try:
                    # Try numeric
                    in_list = ", ".join([str(float(v)) for v in values])
                except (ValueError, TypeError):
                    # Fall back to string with proper escaping
                    in_list = ", ".join([f"'{str(v).replace(chr(39), chr(39)+chr(39))}'" for v in values])
                
                where_clauses.append(f"{col} IN ({in_list})")
        
        if where_clauses:
            return " WHERE " + " AND ".join(where_clauses), where_clauses
        else:
            return "", []
    except Exception as e:
        logger.error(f"Error building WHERE clause: {e}")
        return "", []


def truncate_text(text: str, max_length: int = 50) -> str:
    """Truncate text to max length."""
    if len(text) > max_length:
        return text[:max_length-3] + "..."
    return text
