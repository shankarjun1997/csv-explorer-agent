import streamlit as st
import duckdb_engine as db
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
from pathlib import Path

# Configure page with DARK THEME
st.set_page_config(
    page_title="CSV/PDF Explorer Agent",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'About': "### CSV/PDF Explorer Agent\nExplore & analyze CSV/PDF files locally with DuckDB"
    }
)

# Apply dark theme
st.markdown("""
<style>
    body {
        background-color: #0e1117;
        color: #c9d1d9;
    }
    
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        background: linear-gradient(135deg, #58a6ff, #79c0ff);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        margin-bottom: 0.5rem;
    }
    
    .sub-header {
        font-size: 1rem;
        color: #8b949e;
        margin-bottom: 1.5rem;
    }
    
    .metric-box {
        background: linear-gradient(135deg, #1f6feb, #388bfd);
        color: white;
        padding: 1.5rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
        border: 1px solid #30363d;
    }
    
    .info-box {
        background-color: #0d1117;
        border-left: 4px solid #58a6ff;
        padding: 1rem;
        border-radius: 0.4rem;
        margin: 1rem 0;
        border: 1px solid #30363d;
    }
    
    .success-box {
        background-color: #0d1117;
        border-left: 4px solid #3fb950;
        padding: 1rem;
        border-radius: 0.4rem;
        border: 1px solid #30363d;
    }
</style>
""", unsafe_allow_html=True)

st.markdown('<h1 class="main-header">📊 CSV/PDF Explorer Agent</h1>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">💡 Upload, filter, visualize, analyze & export your data with advanced analytics</p>', unsafe_allow_html=True)

# Initialize the database on startup
if 'db_initialized' not in st.session_state:
    db.init_db()
    st.session_state.db_initialized = True
    st.session_state.filters = {}

# ======================
# SIDEBAR: File Upload
# ======================
with st.sidebar:
    st.markdown("### 📁 Data Management")
    
    # File uploader for CSV and PDF
    uploaded_files = st.file_uploader(
        "📤 Upload Files (CSV or PDF)",
        type=["csv", "pdf"],
        accept_multiple_files=True,
        help="Select one or multiple CSV/PDF files to analyze"
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
            if file.name.lower().endswith('.pdf'):
                status_msg = db.ingest_pdf(file_path)
            else:
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
    
    # Display database info
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
        
        # Show columns with types
        with st.expander("🔍 View Columns", expanded=False):
            col_types = db.get_column_types()
            for i, col in enumerate(col_names, 1):
                col_type = col_types.get(col, "Unknown")
                st.text(f"{i}. {col} ({col_type})")
    else:
        st.info("""
        ### 🚀 Get Started
        1. Upload CSV/PDF files
        2. Explore with filters
        3. Create charts
        4. Detect outliers
        5. Export results
        """)
    
    # Filters section
    if db.table_exists():
        st.markdown("---")
        st.markdown("### 🔍 Filters")
        st.caption("AND logic between columns")
        
        column_names = db.get_column_names()
        filters = {}
        
        for col in column_names:
            unique_vals = db.get_unique_values(col)
            
            if len(unique_vals) <= 50:
                selected_vals = st.multiselect(
                    col,
                    options=unique_vals,
                    key=f"filter_{col}",
                    max_selections=None
                )
                if selected_vals:
                    filters[col] = selected_vals
            else:
                st.caption(f"⚠️ {col}: {len(unique_vals)} values")
        
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
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📋 Data Preview",
        "📊 Charts & Visualization",
        "🎯 Outlier Detection",
        "⚡ SQL Query",
        "📈 Statistics",
        "💾 Export"
    ])
    
    # Helper function for filtering
    def get_filtered_data(limit=1000):
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
            full_query = base_query + " WHERE " + " AND ".join(where_clauses)
        else:
            full_query = base_query
        
        return full_query + f" LIMIT {limit}"
    
    # -------- TAB 1: DATA PREVIEW --------
    with tab1:
        st.markdown("### Explore Your Data")
        
        display_query = get_filtered_data(1000)
        result_df, error = db.run_query(display_query)
        
        if error:
            st.error(f"❌ Query Error: {error}")
        else:
            col1, col2, col3 = st.columns([1, 1, 2])
            with col1:
                st.metric("📊 Rows", len(result_df))
            with col2:
                st.metric("📈 Columns", len(result_df.columns))
            with col3:
                if len(st.session_state.filters) > 0:
                    active_filters = ", ".join(st.session_state.filters.keys())
                    st.info(f"🔍 **Filters:** {active_filters}")
            
            st.markdown("---")
            st.dataframe(result_df, use_container_width=True, height=400, hide_index=True)
    
    # -------- TAB 2: CHARTS & VISUALIZATION --------
    with tab2:
        st.markdown("### Data Visualization")
        
        numeric_cols = db.get_numeric_columns()
        date_cols = db.get_date_columns()
        all_cols = db.get_column_names()
        
        if not numeric_cols:
            st.warning("⚠️ No numeric columns found for charting")
        else:
            chart_type = st.radio(
                "Choose visualization type:",
                ["📊 Histogram", "📈 Line Chart", "🔵 Scatter Plot", "📦 Box Plot", "🍰 Pie Chart", "📊 Bar Chart"],
                horizontal=True
            )
            
            result_df, _ = db.run_query(get_filtered_data())
            
            if chart_type == "📊 Histogram":
                col = st.selectbox("Select numeric column:", numeric_cols)
                fig = px.histogram(
                    result_df,
                    x=col,
                    nbins=30,
                    template="plotly_dark",
                    title=f"Distribution of {col}"
                )
                st.plotly_chart(fig, use_container_width=True)
            
            elif chart_type == "📈 Line Chart":
                col1 = st.selectbox("X-axis (numeric or date):", all_cols)
                col2 = st.selectbox("Y-axis (numeric):", numeric_cols)
                fig = px.line(
                    result_df.sort_values(col1),
                    x=col1,
                    y=col2,
                    template="plotly_dark",
                    title=f"{col2} over {col1}"
                )
                st.plotly_chart(fig, use_container_width=True)
            
            elif chart_type == "🔵 Scatter Plot":
                col1 = st.selectbox("X-axis:", numeric_cols)
                col2 = st.selectbox("Y-axis:", numeric_cols, key="scatter_y")
                fig = px.scatter(
                    result_df,
                    x=col1,
                    y=col2,
                    template="plotly_dark",
                    title=f"{col2} vs {col1}",
                    hover_data=all_cols[:5]
                )
                st.plotly_chart(fig, use_container_width=True)
            
            elif chart_type == "📦 Box Plot":
                col = st.selectbox("Select numeric column:", numeric_cols, key="box_col")
                group_by = st.selectbox("Group by (optional):", ["None"] + all_cols)
                
                if group_by == "None":
                    fig = go.Figure(data=[go.Box(y=result_df[col], template="plotly_dark")])
                    fig.update_layout(title=f"Box Plot of {col}")
                else:
                    fig = px.box(
                        result_df,
                        y=col,
                        x=group_by,
                        template="plotly_dark",
                        title=f"Box Plot of {col} by {group_by}"
                    )
                st.plotly_chart(fig, use_container_width=True)
            
            elif chart_type == "🍰 Pie Chart":
                col = st.selectbox("Select column:", all_cols, key="pie_col")
                fig = px.pie(
                    result_df,
                    names=col,
                    template="plotly_dark",
                    title=f"Distribution of {col}"
                )
                st.plotly_chart(fig, use_container_width=True)
            
            elif chart_type == "📊 Bar Chart":
                col1 = st.selectbox("X-axis:", all_cols, key="bar_x")
                col2 = st.selectbox("Y-axis (numeric):", numeric_cols, key="bar_y")
                
                fig = px.bar(
                    result_df.groupby(col1)[col2].sum().reset_index(),
                    x=col1,
                    y=col2,
                    template="plotly_dark",
                    title=f"{col2} by {col1}"
                )
                st.plotly_chart(fig, use_container_width=True)
    
    # -------- TAB 3: OUTLIER DETECTION --------
    with tab3:
        st.markdown("### Outlier Detection")
        
        numeric_cols = db.get_numeric_columns()
        
        if not numeric_cols:
            st.warning("⚠️ No numeric columns for outlier detection")
        else:
            col1, col2 = st.columns([2, 1])
            
            with col1:
                col_to_analyze = st.selectbox("Select column to analyze:", numeric_cols)
            with col2:
                method = st.radio("Method:", ["IQR", "Z-Score"], horizontal=True)
            
            if st.button("🔍 Detect Outliers", use_container_width=True, type="primary"):
                outlier_df, error = db.detect_outliers(col_to_analyze, "iqr" if method == "IQR" else "zscore")
                
                if error:
                    st.error(f"Error: {error}")
                else:
                    outlier_count = outlier_df['is_outlier'].sum()
                    normal_count = len(outlier_df) - outlier_count
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Total Records", len(outlier_df))
                    with col2:
                        st.metric("🚨 Outliers", outlier_count)
                    with col3:
                        st.metric("✅ Normal", normal_count)
                    
                    st.markdown("---")
                    
                    # Display outliers
                    st.subheader("Detected Outliers")
                    outliers_only = outlier_df[outlier_df['is_outlier'] == True]
                    
                    if len(outliers_only) > 0:
                        st.dataframe(outliers_only, use_container_width=True, hide_index=True)
                        
                        # Visualization
                        fig = px.scatter(
                            outlier_df,
                            x=outlier_df.index,
                            y=col_to_analyze,
                            color='is_outlier',
                            color_discrete_map={True: '#f85149', False: '#58a6ff'},
                            title=f"Outliers in {col_to_analyze}",
                            template="plotly_dark"
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.success("✅ No outliers detected!")
    
    # -------- TAB 4: SQL QUERY --------
    with tab4:
        st.markdown("### Advanced SQL Query")
        st.caption("💡 Write custom SQL queries using DuckDB syntax")
        
        col_names = db.get_column_names()
        
        # Show schema
        with st.expander("📋 Table Schema", expanded=False):
            col_types = db.get_column_types()
            schema_info = "\n".join([f"• {col} ({col_types.get(col, 'Unknown')})" for col in col_names])
            st.markdown(f"**Columns:**\n{schema_info}")
        
        custom_sql = st.text_area(
            "✍️ Enter your SQL query:",
            height=150,
            placeholder="SELECT * FROM csv_data WHERE ...",
            value="SELECT * FROM csv_data LIMIT 100"
        )
        
        col1, col2 = st.columns([1, 4])
        with col1:
            run_query_btn = st.button("🚀 Execute", use_container_width=True, type="primary")
        with col2:
            if st.button("🔄 Reset Query", use_container_width=True):
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
    
    # -------- TAB 5: STATISTICS --------
    with tab5:
        st.markdown("### Statistical Summary")
        
        result_df, _ = db.run_query(get_filtered_data())
        
        numeric_cols = db.get_numeric_columns()
        
        if numeric_cols:
            selected_cols = st.multiselect(
                "Select columns for statistics:",
                numeric_cols,
                default=numeric_cols[:min(3, len(numeric_cols))]
            )
            
            if selected_cols:
                stats_df = result_df[selected_cols].describe().T
                st.dataframe(stats_df, use_container_width=True)
                
                st.markdown("---")
                st.subheader("Correlation Matrix")
                
                if len(selected_cols) > 1:
                    corr_matrix = result_df[selected_cols].corr()
                    fig = px.imshow(
                        corr_matrix,
                        color_continuous_scale="RdBu_r",
                        template="plotly_dark",
                        title="Correlation Matrix"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Select at least 2 columns to see correlation")
        else:
            st.warning("⚠️ No numeric columns available")
    
    # -------- TAB 6: EXPORT --------
    with tab6:
        st.markdown("### Export Your Data")
        st.caption("💾 Download filtered data or custom query results")
        
        export_option = st.radio(
            "What would you like to export?",
            ["Filtered Data", "Custom SQL Query"],
            horizontal=True
        )
        
        if export_option == "Filtered Data":
            export_query = get_filtered_data(limit=999999)
        else:
            export_query = st.text_area(
                "Enter your SQL query:",
                height=150,
                placeholder="SELECT * FROM csv_data WHERE ...",
                value="SELECT * FROM csv_data LIMIT 1000"
            )
        
        if st.button("📥 Prepare Export", use_container_width=True, type="primary"):
            with st.spinner("⏳ Preparing export..."):
                result_df, error = db.run_query(export_query)
            
            if error:
                st.error(f"❌ Error: {error}")
            else:
                st.success(f"✅ Ready! {len(result_df):,} rows prepared")
                
                col1, col2, col3 = st.columns(3)
                
                # CSV download
                with col1:
                    csv = result_df.to_csv(index=False)
                    st.download_button(
                        label="📥 CSV",
                        data=csv,
                        file_name="exported_data.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
                
                # Excel download
                with col2:
                    try:
                        import io
                        excel_buffer = io.BytesIO()
                        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                            result_df.to_excel(writer, sheet_name='Data', index=False)
                        excel_buffer.seek(0)
                        
                        st.download_button(
                            label="📊 Excel",
                            data=excel_buffer.getvalue(),
                            file_name="exported_data.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True
                        )
                    except ImportError:
                        st.caption("Excel not available")
                
                # JSON download
                with col3:
                    json_data = result_df.to_json(orient='records', indent=2)
                    st.download_button(
                        label="🔗 JSON",
                        data=json_data,
                        file_name="exported_data.json",
                        mime="application/json",
                        use_container_width=True
                    )
                
                # Preview
                with st.expander("👁️ Preview (First 50 rows)", expanded=False):
                    st.dataframe(result_df.head(50), use_container_width=True, hide_index=True)

else:
    # Welcome screen
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("""
        ## 🎯 Welcome to CSV/PDF Explorer Agent!
        
        ### Features:
        
        **📊 Data Exploration**
        - Multi-file upload (CSV & PDF)
        - Smart filtering
        - Real-time data preview
        
        **📈 Visualization**
        - Histograms, line charts, scatter plots
        - Box plots, pie charts, bar charts
        - Interactive Plotly charts
        
        **🎯 Advanced Analytics**
        - Outlier detection (IQR & Z-Score)
        - Statistical summary
        - Correlation analysis
        
        **⚡ Query & Export**
        - SQL query builder
        - Export as CSV, Excel, JSON
        - Large dataset support
        """)
    
    with col2:
        st.markdown("""
        ### 💡 Quick Start
        
        1. Upload files
        2. Filter data
        3. Create charts
        4. Detect outliers
        5. Export results
        
        ### 📚 Resources
        
        [DuckDB](https://duckdb.org/)
        [SQL Tutorial](https://w3schools.com/sql)
        """)
    
    st.markdown("---")
    st.info("👈 **Ready?** Upload CSV/PDF files using the sidebar to begin!")
