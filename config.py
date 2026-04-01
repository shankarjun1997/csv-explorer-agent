"""Application configuration and settings."""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Paths
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "my_db.duckdb"

# Create data directory if it doesn't exist
DATA_DIR.mkdir(exist_ok=True)

# Database
DATABASE_PATH = str(DB_PATH)
DATABASE_READ_ONLY = False

# App Settings
APP_NAME = "Enterprise Data Explorer"
APP_VERSION = "2.0.0"
APP_DESCRIPTION = "Professional-grade CSV/PDF data analysis platform"

# Theme Settings
DARK_MODE = True
THEME_COLOR_PRIMARY = "#58a6ff"
THEME_COLOR_SECONDARY = "#79c0ff"
THEME_COLOR_SUCCESS = "#3fb950"
THEME_COLOR_ERROR = "#f85149"
THEME_COLOR_WARNING = "#d29922"

# Data Settings
MAX_ROWS_DISPLAY = 1000
MAX_ROWS_EXPORT = 999999
MAX_UNIQUE_VALUES = 50
CHART_MAX_POINTS = 5000

# Outlier Detection
OUTLIER_IQR_MULTIPLIER = 1.5
OUTLIER_ZSCORE_THRESHOLD = 3

# API Settings
ENABLE_API = False
API_PORT = 8000

# Logging
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# Feature Flags
ENABLE_PDF_SUPPORT = True
ENABLE_CHARTS = True
ENABLE_OUTLIER_DETECTION = True
ENABLE_STATISTICS = True
ENABLE_EXPORT = True
ENABLE_SQL_EDITOR = True
