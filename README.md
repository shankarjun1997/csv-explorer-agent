# 📊 CSV Explorer Agent

A local web application for exploring, filtering, and analyzing CSV data. Built with **Streamlit** + **DuckDB**.

## Features

✨ **Data Ingestion:** Upload single or multiple CSV files  
🔍 **Smart Filtering:** Multi-select filters for all columns  
📋 **Data Preview:** View filtered results with instant updates  
⚡ **Advanced Queries:** Write custom SQL queries using DuckDB syntax  
💾 **Export:** Download results as CSV or Excel  
📊 **Database Info:** View row counts and column information

## Tech Stack

- **Frontend:** [Streamlit](https://streamlit.io/) - Rapid data apps in Python
- **Database:** [DuckDB](https://duckdb.org/) - In-process SQL OLAP database (no server needed)
- **Language:** Python 3.8+

## Quick Start

### 1. Initialize Virtual Environment

```bash
# Navigate to the project directory
cd csv_explorer_agent

# Create a Python virtual environment
python -m venv venv

# Activate the environment
# On macOS/Linux:
source venv/bin/activate

# On Windows:
# venv\Scripts\activate
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Run the Application

```bash
streamlit run app.py
```

The app will open in your browser at **`http://localhost:8501`**

## Project Structure

```
csv_explorer_agent/
├── app.py                    # Main Streamlit application
├── duckdb_engine.py          # All DuckDB database functions
├── requirements.txt          # Python dependencies
├── .env                      # Environment variables (optional)
├── .gitignore               # Git ignore rules
├── data/                    # Folder for uploaded CSV files
│   └── my_db.duckdb        # DuckDB database file (auto-created)
└── README.md                # This file
```

## Usage

### 📁 Uploading Files

1. Open the app in your browser
2. In the **left sidebar**, use "Choose CSV files" to upload one or more CSV files
3. The app will automatically:
   - Save the files to the `data/` folder
   - Parse them with Pandas
   - Ingest them into the DuckDB database
   - Display a success message

### 🔍 Filtering Data

1. After uploading, the sidebar shows all available columns
2. Use the multi-select dropdowns to filter data
3. The **Data Preview** tab updates in real-time
4. Select multiple values to create an **OR** filter within a column
5. Multiple columns create an **AND** filter

### 📋 Viewing Data

**Tab 1 - Data Preview:**

- Shows all filtered data (limited to 1000 rows for performance)
- Displays row/column counts
- Sortable columns

### ⚡ Advanced Queries

**Tab 2 - Advanced Query:**

- Write custom SQL queries
- Schema info popup shows all available columns
- Full DuckDB SQL syntax support (CTEs, window functions, aggregations, etc.)

**Example queries:**

```sql
-- Group by and aggregate
SELECT column1, COUNT(*) as count FROM csv_data GROUP BY column1 ORDER BY count DESC;

-- Date filtering (if you have date columns)
SELECT * FROM csv_data WHERE date_column >= '2023-01-01';

-- String matching
SELECT * FROM csv_data WHERE column1 ILIKE '%search_term%';

-- Top N results
SELECT * FROM csv_data ORDER BY numeric_column DESC LIMIT 10;
```

### 💾 Export Data

**Tab 3 - Export:**

- Export filtered data or custom query results
- Download as CSV or Excel (.xlsx)
- Preview data before exporting

## DuckDB Syntax Guide

DuckDB is fully SQL:2003 compliant with some extensions:

```sql
-- Basic SELECT
SELECT * FROM csv_data LIMIT 100;

-- Filter with WHERE
SELECT * FROM csv_data WHERE column1 = 'value';

-- String matching (case-insensitive)
SELECT * FROM csv_data WHERE column1 ILIKE '%text%';

-- Aggregate functions
SELECT column1, COUNT(*), AVG(numeric_column)
FROM csv_data
GROUP BY column1;

-- Join multiple columns
SELECT * FROM csv_data
WHERE column1 IN ('value1', 'value2');

-- Sort and limit
SELECT * FROM csv_data
ORDER BY numeric_column DESC
LIMIT 50;
```

For more syntax details, see [DuckDB Documentation](https://duckdb.org/docs/)

## Troubleshooting

### Issue: "No data loaded"

**Solution:** Make sure you've uploaded CSV files using the sidebar uploader

### Issue: "Query Error: Table 'csv_data' does not exist"

**Solution:** Upload at least one CSV file first

### Issue: Slow performance with large files

**Solution:**

- DuckDB can handle millions of rows, but previewing is limited to 1000 rows for UI speed
- Use `LIMIT` in your SQL queries
- Export filtered results instead of viewing entire dataset

### Issue: Memory issues

**Solution:**

- DuckDB is in-process, so performance depends on available RAM
- For very large datasets (>10GB), consider using a dedicated database server

## Next Steps (Future Enhancements)

Phase 3 & 4 (when ready):

- ✨ Add AI Query Agent (GPT-4o-mini integration for natural language queries)
- 📊 Add visualization library (Plotly/Altair for charts)
- 🚀 Deploy to cloud (Streamlit Cloud, Heroku, etc.)

## FAQ

**Q: Can I upload files from different sources (not just local)?**
A: Yes! You can upload any CSV file as long as it's readable by Pandas.

**Q: Can I edit data in the app?**
A: Currently, the app is read-only. Future versions could add edit/delete functionality.

**Q: How do I reset the database?**
A: Delete the `data/my_db.duckdb` file and restart the app.

**Q: Can I use this with large datasets?**
A: Yes! DuckDB can handle datasets with billions of rows. However, UI preview is limited to 1000 rows for performance.

## License

Free to use and modify!

---

Built with ❤️ using Streamlit + DuckDB
