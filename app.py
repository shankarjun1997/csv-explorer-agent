import streamlit as st
import duckdb_engine as db
import pandas as pd
import os
from pathlib import Path

# Configure page
st.set_page_config(
    page_title="CSV Explorer Agent",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'About': "### CSV Explorer Agent\nExplore & analyze CSV files locally with DuckDB"
    }
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1f77b4;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1rem;
        color: #666;
        margin-bottom: 1.5rem;
    }
    .metric-box {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1.5rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .info-box {
        background-color: #e8f4f8;
        border-left: 4px solid #0288d1;
        padding: 1rem;
        border-radius: 0.4rem;
        margin: 1rem 0;
    }
    .success-box {
        background-color: #e8f5e9;
        border-left: 4px solid #4caf50;
        padding: 1rem;
        border-radius: 0.4rem;
    }
</style>
""", unsafe_allow_html=True)

st.markdown('<h1 class="main-header">📊 CSV Explorer Agent</h1>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">💡 Upload, filter, query & export your CSV data with ease</p>', unsafe_allow_html=True)

# Initialize the database on startup
if 'db_initialized' not in st.session_state:
    db.init_db()
    st.session_state.db_initialized = True
    st.session_state.filters = {}
# SIDEBAR: File Upload
# ======================
with st.sidebar:
    st.markdown("### 📁 Data Management")
    
    # File uploader with better UI
    uploaded_files = st.file_uploader(
        "📤 Upload CSV Files",
        type="csv",
        accept_multiple_files=True,
        help="Select one or multiple CSV files to analyze"
    )
    
    if uploaded_files:
        st.markdown("---")
        st.markdown("#### ⏳ Processing Files...")
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        successful = 0
        for idx, file in enumerate(uploaded_files):
            status_text.text(f"Uploading {file.name}...")
            
            # Save file to data directory
            data_dir = os.path.join(os.path.dirname(__file__), "data")
            os.makedirs(data_dir, exist_ok=True)
            file_path = os.path.join(data_dir, file.name)
            
            with open(file_path, "wb") as f:
                f.write(file.getbuffer())
            
            # Ingest into DuckDB
            status_msg = db.ingest_csv(file_path)
            
            if "✅" in status_msg:
                successful += 1
            
            progress_bar.progress((idx + 1) / len(uploaded_files))
        
        status_text.empty()
        progress_bar.empty()
        
        if successful == len(uploaded_files):
            st.success(f"✅ Successfully loaded {successful} file(s)!")
        else:
            st.warning(f"⚠️ {successful}/{len(uploaded_files)} files loaded. Check for errors above.")
    
    # Display database info with better styling
    st.markdown("---")
    st.markdown("### 📊 Database Status")
    
    if db.table_exists():
        col_names = db.get_column_names()
        row_count = db.get_row_count()
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("📈 Total Rows", f"{row_count:,}")
        with col2:
            st.metric("📋 Columns", len(col_names))
        
        # Show columns
        with st.expander("🔍 View Columns", expanded=False):
            for i, col in enumerate(col_names, 1):
                st.text(f"{i}. {col}")
    else:
        st.info("""
        ### 🚀 Get Started
        1. Upload CSV files using the button above
        2. Use filters to explore data
        3. Write SQL queries for advanced analysis
        """)
    
    # Filters section - improved
    if db.table_exists():
        st.markdown("---")
        st.markdown("### 🔍 Filters")
        st.caption("Select values to filter data (AND logic between columns)")
        
        column_names = db.get_column_names()
        filters = {}
        
        for col in column_names:
            unique_vals = db.get_unique_values(col)
            
            if len(unique_vals) <= 50:  # Only show multiselect for reasonable cardinality
                selected_vals = st.multiselect(
                    col,
                    options=unique_vals,
                    key=f"filter_{col}",
                    max_selections=None
                )
                if selected_vals:
                    filters[col] = selected_vals
            else:
                st.caption(f"⚠️ {col} has {len(unique_vals)} unique values - too many to filter")
        
        st.session_state.filters = filters
        
        # Clear filters button
        if filters:
            if st.button("🗑️ Clear All Filters", use_container_width=True):
                for key in list(st.session_state.keys()):
                    if key.startswith("filter_"):
                        del st.session_state[key]
                st.rerun()

# ======================
# MAIN CONTENT: Tabs
# ======================
if db.table_exists():
    tab1, tab2, tab3 = st.tabs(["📋 Data Preview", "⚡ SQL Query", "💾 Export Data"])
    
    # -------- TAB 1: DATA PREVIEW --------
    with tab1:
        st.markdown("### Explore Your Data")
        
        # Build filtered query
        base_query = "SELECT * FROM csv_data"
        where_clauses = []
        
        for col, values in st.session_state.filters.items():
            if values:
                # Handle different data types
                try:
                    # Try numeric
                    in_list = ", ".join([str(float(v)) for v in values])
                except:
                    # Fall back to string
                    in_list = ", ".join([f"'{str(v).replace(chr(39), chr(39)+chr(39))}'" for v in values])
                
                where_clauses.append(f"{col} IN ({in_list})")
        
        if where_clauses:
            full_query = base_query + " WHERE " + " AND ".join(where_clauses)
        else:
            full_query = base_query
        
        # Add limit for display
        display_query = full_query + " LIMIT 1000"
        
        # Run the query
        result_df, error = db.run_query(display_query)
        
        if error:
            st.error(f"❌ Query Error: {error}")
        else:
            # Display results with metrics
            col1, col2, col3 = st.columns([1, 1, 2])
            with col1:
                st.metric("📊 Rows", len(result_df))
            with col2:
                st.metric("📈 Columns", len(result_df.columns))
            with col3:
                if len(st.session_state.filters) > 0:
                    active_filters = ", ".join(st.session_state.filters.keys())
                    st.info(f"🔍 **Filters Active:** {active_filters}")
            
            st.markdown("---")
            
            # Scrollable dataframe
            st.dataframe(
                result_df,
                use_container_width=True,
                height=400,
                hide_index=True
            )
            
            # Summary stats
            col_summary = st.columns([1, 1, 1])
            with col_summary[0]:
                if len(st.session_state.filters) > 0:
                    st.caption(f"📌 Showing filtered results")
                else:
                    st.caption(f"📌 Showing first 1000 rows")
    
    # -------- TAB 2: SQL QUERY --------
    with tab2:
        st.markdown("### Advanced SQL Query")
        st.caption("💡 Write custom SQL queries using DuckDB syntax. Table name: **csv_data**")
        
        col_names = db.get_column_names()
        
        # Show schema in expandable
        with st.expander("📋 View Table Schema", expanded=False):
            schema_text = "\n".join([f"• {col}" for col in col_names])
            st.markdown(f"**Columns in csv_data:**\n{schema_text}")
            
            st.markdown("### Example Queries:")
            examples = [
                "```sql\nSELECT * FROM csv_data LIMIT 50;\n```",
                "```sql\nSELECT COUNT(*) as total FROM csv_data;\n```",
                "```sql\nSELECT * FROM csv_data WHERE column_name = 'value';\n```",
                "```sql\nSELECT column_name, COUNT(*) as count FROM csv_data GROUP BY column_name ORDER BY count DESC;\n```"
            ]
            for example in examples:
                st.markdown(example)
        
        # Query input
        custom_sql = st.text_area(
            "✍️ Enter your SQL query:",
            height=180,
            placeholder="SELECT * FROM csv_data WHERE ...",
            value="SELECT * FROM csv_data LIMIT 100",
            key="sql_query"
        )
        
        col1, col2, col3 = st.columns([1, 4, 1])
        with col1:
            run_query_btn = st.button("🚀 Execute", use_container_width=True, type="primary")
        with col3:
            clear_btn = st.button("🔄 Reset", use_container_width=True)
        
        if clear_btn:
            st.session_state.sql_query = "SELECT * FROM csv_data LIMIT 100"
            st.rerun()
        
        if run_query_btn:
            if not custom_sql.strip():
                st.warning("⚠️ Please enter a SQL query")
            else:
                with st.spinner("⏳ Executing query..."):
                    result_df, error = db.run_query(custom_sql)
                
                if error:
                    st.error(f"❌ Query Failed")
                    st.code(f"Error: {error}", language="text")
                else:
                    st.success(f"✅ Success! Found {len(result_df):,} rows")
                    
                    col1, col2 = st.columns([1, 1])
                    with col1:
                        st.metric("📊 Rows", len(result_df))
                    with col2:
                        st.metric("📈 Columns", len(result_df.columns))
                    
                    st.markdown("---")
                    st.dataframe(result_df, use_container_width=True, hide_index=True)
    
    # -------- TAB 3: EXPORT --------
    with tab3:
        st.markdown("### Export Your Data")
        st.caption("💾 Download filtered data or custom query results")
        
        export_option = st.radio(
            "What would you like to export?",
            [
                "Filtered Data",
                "Custom SQL Query"
            ],
            horizontal=True
        )
        
        if export_option == "Filtered Data":
            base_query = "SELECT * FROM csv_data"
            where_clauses = []
            
            for col, values in st.session_state.filters.items():
                if values:
                    try:
                        in_list = ", ".join([str(float(v)) for v in values])
                    except:
                        in_list = ", ".join([f"'{str(v).replace(chr(39), chr(39)+chr(39))}'" for v in values])
                    where_clauses.append(f"{col} IN ({in_list})")
            
            if where_clauses:
                export_query = base_query + " WHERE " + " AND ".join(where_clauses)
            else:
                export_query = base_query
                
            st.info(f"**Query:** `{export_query[:100]}...`" if len(export_query) > 100 else f"**Query:** `{export_query}`")
            
        else:
            export_query = st.text_area(
                "Enter your SQL query:",
                height=150,
                placeholder="SELECT * FROM csv_data WHERE ...",
                value="SELECT * FROM csv_data LIMIT 1000"
            )
        
        col1, col2 = st.columns([1, 3])
        with col1:
            export_btn = st.button("📥 Prepare Export", use_container_width=True, type="primary")
        
        if export_btn:
            with st.spinner("⏳ Preparing export..."):
                result_df, error = db.run_query(export_query)
            
            if error:
                st.error(f"❌ Error: {error}")
            else:
                st.success(f"✅ Ready! {len(result_df):,} rows prepared")
                
                col_exp1, col_exp2 = st.columns(2)
                
                # CSV download
                with col_exp1:
                    csv = result_df.to_csv(index=False)
                    st.download_button(
                        label="📥 Download CSV",
                        data=csv,
                        file_name="exported_data.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
                
                # Excel download (if openpyxl available)
                with col_exp2:
                    try:
                        import openpyxl
                        import io
                        
                        excel_buffer = io.BytesIO()
                        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                            result_df.to_excel(writer, sheet_name='Data', index=False)
                        excel_buffer.seek(0)
                        
                        st.download_button(
                            label="📊 Download Excel",
                            data=excel_buffer.getvalue(),
                            file_name="exported_data.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True
                        )
                    except ImportError:
                        st.caption("💡 Install `openpyxl` for Excel export")
                
                # Preview
                with st.expander("👁️ Preview Data (First 100 rows)", expanded=False):
                    st.dataframe(result_df.head(100), use_container_width=True, hide_index=True)

else:
    # Welcome screen when no data
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("""
        ## 🎯 Welcome to CSV Explorer Agent!
        
        ### Get Started in 3 Steps:
        
        **1️⃣ Upload Files**
        - Use the sidebar to upload one or more CSV files
        - Supported formats: .csv
        
        **2️⃣ Explore Data**
        - Use filters to narrow down your data
        - View results instantly
        
        **3️⃣ Query & Export**
        - Write SQL queries for advanced analysis
        - Export results as CSV or Excel
        
        ### Key Features:
        - ✨ Multi-file upload
        - 🔍 Smart filtering
        - ⚡ SQL query builder
        - 💾 Export functionality
        - 📊 Real-time insights
        """)
    
    with col2:
        st.markdown("""
        ### 💡 Tips
        
        - Start with small files
        - Use filters for quick analysis
        - Check the schema before writing queries
        - Export filtered results
        
        ### Resources
        
        [DuckDB Docs →](https://duckdb.org/docs/)
        
        [SQL Tutorial →](https://www.w3schools.com/sql/)
        """)
    
    st.markdown("---")
    st.info("👈 **Ready?** Upload CSV files using the sidebar to begin!")
