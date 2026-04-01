"""
Enterprise Data Explorer - Professional-grade CSV/PDF analysis platform
Built with Streamlit, DuckDB, and modern enterprise UI/UX
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
from datetime import datetime
from config import APP_NAME, APP_VERSION, APP_DESCRIPTION
from duckdb_engine import db
from logger import setup_logger
from utils import (
    format_number, format_file_size, 
    get_data_quality_metrics, get_column_summary,
    build_where_clause, truncate_text
)

logger = setup_logger(__name__)

# ============================================================================
# PAGE CONFIGURATION
# ============================================================================

st.set_page_config(
    page_title=f"{APP_NAME} v{APP_VERSION}",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'About': f"### {APP_NAME}\n**Version** {APP_VERSION}\n\n{APP_DESCRIPTION}"
    }
)

# ============================================================================
# THEME & STYLING
# ============================================================================

st.markdown("""
<style>
    /* Main background */
    .main { background-color: #0d1117; color: #c9d1d9; }
    
    /* Headers */
    .main-title {
        font-size: 3rem;
        font-weight: 800;
        background: linear-gradient(135deg, #58a6ff 0%, #79c0ff 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        margin-bottom: 0.5rem;
        letter-spacing: -1px;
    }
    
    .section-title {
        font-size: 1.8rem;
        color: #58a6ff;
        font-weight: 700;
        border-bottom: 2px solid #30363d;
        padding-bottom: 0.75rem;
        margin: 2rem 0 1rem 0;
    }
    
    .subsection-title {
        font-size: 1.3rem;
        color: #79c0ff;
        font-weight: 600;
        margin: 1.5rem 0 1rem 0;
    }
    
    /* Cards & Boxes */
    .metric-card {
        background: linear-gradient(135deg, #21262d 0%, #161b22 100%);
        border: 1px solid #30363d;
        border-radius: 0.75rem;
        padding: 1.5rem;
        margin: 0.75rem 0;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.3);
    }
    
    .info-card {
        background-color: #0d1117;
        border-left: 4px solid #58a6ff;
        border: 1px solid #30363d;
        padding: 1.25rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
        box-shadow: 0 1px 4px rgba(0, 0, 0, 0.2);
    }
    
    .warning-card {
        background-color: #0d1117;
        border-left: 4px solid #d29922;
        border: 1px solid #30363d;
        padding: 1.25rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
    }
    
    .success-card {
        background-color: #0d1117;
        border-left: 4px solid #3fb950;
        border: 1px solid #30363d;
        padding: 1.25rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
    }
    
    /* Data quality badge */
    .badge {
        display: inline-block;
        padding: 0.4rem 0.8rem;
        border-radius: 999px;
        font-size: 0.85rem;
        font-weight: 600;
    }
    
    .badge-good { background-color: rgba(63, 185, 80, 0.2); color: #3fb950; }
    .badge-warning { background-color: rgba(210, 153, 34, 0.2); color: #d29922; }
    .badge-danger { background-color: rgba(248, 81, 73, 0.2); color: #f85149; }
    
    /* Sidebar */
    [data-testid="stSidebar"] { background-color: #010409; border-right: 1px solid #30363d; }
    
    /* Tables */
     .stDataFrame { background-color: #0d1117; }
    
    /* Tabs */
    [role="tab"] { color: #8b949e; border-bottom: 2px solid transparent; padding: 0.75rem 1.5rem; }
    [role="tab"][aria-selected="true"] { 
        color: #58a6ff; 
        border-bottom-color: #58a6ff;
        background-color: transparent;
    }
    
    /* Buttons */
    .stButton button {
        background: linear-gradient(135deg, #1f6feb 0%, #388bfd 100%);
        color: white;
        border: none;
        border-radius: 0.5rem;
        font-weight: 600;
        padding: 0.6rem 1.5rem;
        transition: all 0.3s ease;
    }
    
    .stButton button:hover {
        background: linear-gradient(135deg, #268cff 0%, #4a9bff 100%);
        box-shadow: 0 4px 12px rgba(88, 166, 255, 0.3);
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# INITIALIZATION
# ============================================================================

if 'db_initialized' not in st.session_state:
    db.init_db()
    st.session_state.db_initialized = True
    st.session_state.filters = {}

# ============================================================================
# HEADER & BRANDING
# ============================================================================

col_logo, col_title, col_version = st.columns([1, 4, 1])

with col_title:
    st.markdown(f'<h1 class="main-title">📊 {APP_NAME}</h1>', unsafe_allow_html=True)
    st.markdown(f'<p style="color: #8b949e; margin-top: -1rem; font-size: 1rem;">{APP_DESCRIPTION}</p>', unsafe_allow_html=True)

with col_version:
    st.markdown(f'''
    <div style="text-align: right; color: #8b949e; margin-top: 0.5rem;">
        <p style="margin: 0;">v{APP_VERSION}</p>
        <p style="margin: 0; font-size: 0.85rem;">{datetime.now().strftime("%Y-%m-%d")}</p>
    </div>
    ''', unsafe_allow_html=True)

st.markdown("---")

# ============================================================================
# SIDEBAR - DATA MANAGEMENT
# ============================================================================

with st.sidebar:
    st.markdown('<h2 style="color: #58a6ff;">📁 Data Management</h2>', unsafe_allow_html=True)
    
    # File upload section
    with st.expander("📤 Upload Files", expanded=True):
        uploaded_files = st.file_uploader(
            "Choose CSV or PDF files",
            type=["csv", "pdf"],
            accept_multiple_files=True,
            help="Upload one or multiple data files"
        )
        
        if uploaded_files:
            st.markdown("**Processing files...**")
            progress_bar = st.progress(0)
            status_placeholder = st.empty()
            
            successful = 0
            for idx, file in enumerate(uploaded_files):
                status_placeholder.info(f"📥 {truncate_text(file.name)}")
                
                data_dir = os.path.join(os.path.dirname(__file__), "data")
                os.makedirs(data_dir, exist_ok=True)
                file_path = os.path.join(data_dir, file.name)
                
                with open(file_path, "wb") as f:
                    f.write(file.getbuffer())
                
                if file.name.lower().endswith('.pdf'):
                    msg = db.ingest_pdf(file_path)
                else:
                    msg = db.ingest_csv(file_path)
                
                if "✅" in msg:
                    successful += 1
                    status_placeholder.success(msg)
                else:
                    status_placeholder.error(msg)
                
                progress_bar.progress((idx + 1) / len(uploaded_files))
            
            st.success(f"✅ {successful}/{len(uploaded_files)} files loaded successfully!")
    
    # Database status
    st.markdown("---")
    st.markdown('<h3 style="color: #58a6ff;">📊 Database Status</h3>', unsafe_allow_html=True)
    
    if db.table_exists():
        col_names = db.get_column_names()
        row_count = db.get_row_count()
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Rows", f"{row_count:,}")
        with col2:
            st.metric("Columns", len(col_names))
        
        col_types = db.get_column_types()
        numeric_cols = [c for c in col_names if c in col_types]
        
        with st.expander("🔍 Column Details"):
            for col in col_names:
                col_type = col_types.get(col, "Unknown")
                st.caption(f"• **{col}** ({col_type})")
        
        # Data quality metrics
        result_df, _ = db.run_query("SELECT * FROM csv_data LIMIT 10000")
        if result_df is not None:
            quality = get_data_quality_metrics(result_df)
            if quality:
                with st.expander("📈 Data Quality"):
                    col1, col2 = st.columns(2)
                    with col1:
                        completeness = quality.get('completeness', 0)
                        if completeness >= 95:
                            badge = '<span class="badge badge-good">✓ Excellent</span>'
                        elif completeness >= 80:
                            badge = '<span class="badge badge-warning">⚠ Good</span>'
                        else:
                            badge = '<span class="badge badge-danger">✗ Poor</span>'
                        
                        st.markdown(f"**Completeness:** {completeness:.1f}% {badge}", unsafe_allow_html=True)
                    
                    with col2:
                        dupes = quality.get('duplicate_percentage', 0)
                        st.metric("Duplicates", f"{dupes:.1f}%")
        
        # Filters
        st.markdown("---")
        st.markdown('<h3 style="color: #58a6ff;">🔍 Filters</h3>', unsafe_allow_html=True)
        
        col_names = db.get_column_names()
        filters = {}
        
        for col in col_names:
            unique_vals = db.get_unique_values(col)
            
            if len(unique_vals) <= 50:
                selected_vals = st.multiselect(
                    col,
                    options=unique_vals,
                    key=f"filter_{col}",
                )
                if selected_vals:
                    filters[col] = selected_vals
        
        st.session_state.filters = filters
        
        if filters:
            col1, col2 = st.columns(2)
            with col1:
                if st.button("🗑️ Clear Filters", use_container_width=True):
                    for key in list(st.session_state.keys()):
                        if key.startswith("filter_"):
                            del st.session_state[key]
                    st.rerun()
    else:
        st.info("""
        ### 🚀 Getting Started
        1. Upload CSV or PDF files
        2. Explore with filters
        3. Create visualizations
        4. Export results
        """)

# ============================================================================
# MAIN CONTENT
# ============================================================================

if not db.table_exists():
    # Welcome screen
    st.markdown('<h2 class="section-title">Welcome to Enterprise Data Explorer</h2>', unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        ### 📊 **Data Import**
        - CSV & PDF support
        - Auto-detection
        - Bulk upload
        """)
    
    with col2:
        st.markdown("""
        ### 📈 **Analysis**
        - Interactive charts
        - Statistical summary
        - Outlier detection
        """)
    
    with col3:
        st.markdown("""
        ### 💾 **Export**
        - Multiple formats
        - Custom queries
        - Large datasets
        """)
    
    st.markdown("---")
    st.info("👈 **Start by uploading data files in the sidebar**")

else:
    # Main tabs
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📋 Data View",
        "📊 Visualize",
        "🎯 Analyze",
        "⚡ Query",
        "📈 Reports",
        "💾 Export"
    ])
    
    # ========== TAB 1: DATA VIEW ==========
    with tab1:
        st.markdown('<h2 class="section-title">Data Preview</h2>', unsafe_allow_html=True)
        
        # Build filtered query
        where_clause, _ = build_where_clause(st.session_state.filters)
        query = f"SELECT * FROM csv_data {where_clause} LIMIT 1000"
        
        result_df, error = db.run_query(query)
        
        if error:
            st.error(f"❌ Query Error: {error}")
        else:
            # Metrics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("📊 Rows", f"{len(result_df):,}")
            with col2:
                st.metric("📈 Columns", len(result_df.columns))
            with col3:
                st.metric("🔍 Active Filters", len(st.session_state.filters))
            with col4:
                st.metric("📏 Size", format_file_size(result_df.memory_usage(deep=True).sum()))
            
            st.divider()
            
            # Data table
            st.dataframe(result_df, use_container_width=True, height=400, hide_index=True)
    
    # ========== TAB 2: VISUALIZE ==========
    with tab2:
        st.markdown('<h2 class="section-title">Data Visualization</h2>', unsafe_allow_html=True)
        
        numeric_cols = db.get_numeric_columns()
        date_cols = db.get_date_columns()
        all_cols = db.get_column_names()
        
        if not numeric_cols:
            st.warning("⚠️ No numeric columns available for visualization")
        else:
            # Chart type selection
            col1, col2 = st.columns([2, 1])
            with col1:
                chart_type = st.selectbox(
                    "Chart Type",
                    ["📊 Histogram", "📈 Line", "🔵 Scatter", "📦 Box", "🍰 Pie", "📊 Bar"],
                    label_visibility="collapsed"
                )
            
            result_df, _ = db.run_query(f"SELECT * FROM csv_data {build_where_clause(st.session_state.filters)[0]}")
            
            if result_df is not None and len(result_df) > 0:
                if chart_type == "📊 Histogram":
                    col = st.selectbox("Column", numeric_cols, key="hist_col")
                    fig = px.histogram(result_df, x=col, nbins=30, template="plotly_dark", title=f"Distribution of {col}")
                    st.plotly_chart(fig, use_container_width=True)
                
                elif chart_type == "📈 Line":
                    col1_sel = st.selectbox("X-axis", all_cols, key="line_x")
                    col2_sel = st.selectbox("Y-axis", numeric_cols, key="line_y")
                    fig = px.line(result_df.sort_values(col1_sel), x=col1_sel, y=col2_sel, template="plotly_dark")
                    st.plotly_chart(fig, use_container_width=True)
                
                elif chart_type == "🔵 Scatter":
                    col1_sel = st.selectbox("X-axis", numeric_cols, key="scatter_x")
                    col2_sel = st.selectbox("Y-axis", numeric_cols, key="scatter_y")
                    fig = px.scatter(result_df, x=col1_sel, y=col2_sel, template="plotly_dark")
                    st.plotly_chart(fig, use_container_width=True)
                
                elif chart_type == "📦 Box":
                    col_sel = st.selectbox("Column", numeric_cols, key="box_col")
                    fig = px.box(result_df, y=col_sel, template="plotly_dark")
                    st.plotly_chart(fig, use_container_width=True)
                
                elif chart_type == "🍰 Pie":
                    col_sel = st.selectbox("Column", all_cols, key="pie_col")
                    fig = px.pie(result_df, names=col_sel, template="plotly_dark")
                    st.plotly_chart(fig, use_container_width=True)
                
                elif chart_type == "📊 Bar":
                    col1_sel = st.selectbox("X-axis", all_cols, key="bar_x")
                    col2_sel = st.selectbox("Y-axis", numeric_cols, key="bar_y")
                    agg_df = result_df.groupby(col1_sel)[col2_sel].sum().reset_index()
                    fig = px.bar(agg_df, x=col1_sel, y=col2_sel, template="plotly_dark")
                    st.plotly_chart(fig, use_container_width=True)
    
    # ========== TAB 3: ANALYZE ==========
    with tab3:
        st.markdown('<h2 class="section-title">Advanced Analysis</h2>', unsafe_allow_html=True)
        
        analysis_type = st.radio("Analysis Type", ["Outliers", "Statistics", "Correlation"], horizontal=True)
        
        if analysis_type == "Outliers":
            st.markdown('<h3 class="subsection-title">Outlier Detection</h3>', unsafe_allow_html=True)
            numeric_cols = db.get_numeric_columns()
            if numeric_cols:
                col1, col2 = st.columns([2, 1])
                with col1:
                    col_to_analyze = st.selectbox("Column", numeric_cols, key="outlier_col")
                with col2:
                    method = st.radio("Method", ["IQR", "Z-Score"], horizontal=True)
                
                if st.button("🔍 Detect", use_container_width=True, type="primary"):
                    outlier_df, error = db.detect_outliers(col_to_analyze, "iqr" if method == "IQR" else "zscore")
                    if error:
                        st.error(error)
                    else:
                        outlier_count = outlier_df['is_outlier'].sum()
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Total", len(outlier_df))
                        with col2:
                            st.metric("🚨 Outliers", int(outlier_count))
                        with col3:
                            pct = (outlier_count / len(outlier_df) * 100) if len(outlier_df) > 0 else 0
                            st.metric("Percentage", f"{pct:.1f}%")
                        
                        st.divider()
                        outliers_only = outlier_df[outlier_df['is_outlier'] == True]
                        if len(outliers_only) > 0:
                            st.dataframe(outliers_only, use_container_width=True, hide_index=True)
        
        elif analysis_type == "Statistics":
            st.markdown('<h3 class="subsection-title">Statistical Summary</h3>', unsafe_have_html=True)
            numeric_cols = db.get_numeric_columns()
            if numeric_cols:
                selected_cols = st.multiselect("Columns", numeric_cols, default=numeric_cols[:min(3, len(numeric_cols))])
                if selected_cols:
                    result_df, _ = db.run_query(f"SELECT * FROM csv_data {build_where_clause(st.session_state.filters)[0]} LIMIT 10000")
                    if result_df is not None:
                        stats_df = result_df[selected_cols].describe().T
                        st.dataframe(stats_df, use_container_width=True)
        
        elif analysis_type == "Correlation":
            st.markdown('<h3 class="subsection-title">Correlation Matrix</h3>', unsafe_allow_html=True)
            numeric_cols = db.get_numeric_columns()
            if len(numeric_cols) > 1:
                result_df, _ = db.run_query(f"SELECT * FROM csv_data {build_where_clause(st.session_state.filters)[0]} LIMIT 10000")
                if result_df is not None:
                    corr_matrix = result_df[numeric_cols].corr()
                    fig = px.imshow(corr_matrix, color_continuous_scale="RdBu_r", template="plotly_dark")
                    st.plotly_chart(fig, use_container_width=True)
    
    # ========== TAB 4: QUERY ==========
    with tab4:
        st.markdown('<h2 class="section-title">SQL Query Editor</h2>', unsafe_allow_html=True)
        
        col_names = db.get_column_names()
        col_types = db.get_column_types()
        
        with st.expander("📋 Schema Reference"):
            for col in col_names:
                st.caption(f"• {col} ({col_types.get(col, 'Unknown')})")
        
        custom_sql = st.text_area(
            "SQL Query",
            height=180,
            placeholder="SELECT * FROM csv_data WHERE ...",
            value="SELECT * FROM csv_data LIMIT 100",
            label_visibility="collapsed"
        )
        
        col1, col2, col3 = st.columns([1, 1, 2])
        with col1:
            if st.button("🚀 Execute", use_container_width=True, type="primary"):
                with st.spinner("Executing..."):
                    result_df, error = db.run_query(custom_sql)
                if error:
                    st.error(f"❌ Error: {error}")
                else:
                    st.success(f"✅ {len(result_df):,} rows")
                    st.dataframe(result_df, use_container_width=True, hide_index=True)
        
        with col2:
            if st.button("🔄 Reset", use_container_width=True):
                st.rerun()
    
    # ========== TAB 5: REPORTS ==========
    with tab5:
        st.markdown('<h2 class="section-title">Data Reports</h2>', unsafe_allow_html=True)
        
        report_type = st.selectbox(
            "Report Type",
            ["Data Quality", "Summary Statistics", "Completeness Analysis"],
            label_visibility="collapsed"
        )
        
        result_df, _ = db.run_query(f"SELECT * FROM csv_data {build_where_clause(st.session_state.filters)[0]} LIMIT 10000")
        
        if report_type == "Data Quality" and result_df is not None:
            quality = get_data_quality_metrics(result_df)
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Missing Cells", quality.get('missing_cells', 0))
            with col2:
                st.metric("Completeness", f"{quality.get('completeness', 0):.1f}%")
            with col3:
                st.metric("Duplicates", quality.get('duplicate_rows', 0))
            with col4:
                st.metric("Uniqueness", f"{100-quality.get('duplicate_percentage', 0):.1f}%")
        
        elif report_type == "Summary Statistics" and result_df is not None:
            for col in result_df.columns[:5]:
                summary = get_column_summary(result_df[col])
                st.write(f"**{col}**: {summary.get('unique', 0)} unique values")
        
        elif report_type == "Completeness Analysis" and result_df is not None:
            missing_df = pd.DataFrame({
                'Column': result_df.columns,
                'Missing': result_df.isnull().sum(),
                'Missing %': (result_df.isnull().sum() / len(result_df) * 100).round(2)
            })
            st.dataframe(missing_df, use_container_width=True, hide_index=True)
    
    # ========== TAB 6: EXPORT ==========
    with tab6:
        st.markdown('<h2 class="section-title">Export Data</h2>', unsafe_allow_html=True)
        
        export_type = st.radio("Export Type", ["Filtered Data", "Custom Query"], horizontal=True)
        
        if export_type == "Filtered Data":
            export_query = f"SELECT * FROM csv_data {build_where_clause(st.session_state.filters)[0]}"
        else:
            export_query = st.text_area(
                "SQL Query",
                height=100,
                placeholder="SELECT * FROM csv_data",
                value="SELECT * FROM csv_data LIMIT 1000",
                label_visibility="collapsed"
            )
        
        if st.button("📥 Prepare Export", use_container_width=True, type="primary"):
            with st.spinner("Preparing..."):
                result_df, error = db.run_query(export_query)
            
            if error:
                st.error(f"Error: {error}")
            else:
                st.success(f"✅ Ready! {len(result_df):,} rows")
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    csv = result_df.to_csv(index=False)
                    st.download_button("📥 CSV", csv, "data.csv", "text/csv", use_container_width=True)
                with col2:
                    try:
                        import io
                        buf = io.BytesIO()
                        with pd.ExcelWriter(buf, engine='openpyxl') as w:
                            result_df.to_excel(w, index=False)
                        st.download_button("📊 Excel", buf.getvalue(), "data.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
                    except:
                        st.caption("Excel not available")
                with col3:
                    json_data = result_df.to_json(orient='records')
                    st.download_button("🔗 JSON", json_data, "data.json", "application/json", use_container_width=True)

# ============================================================================
# FOOTER
# ============================================================================

st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #8b949e; font-size: 0.85rem; padding: 1rem;">
    <p>Enterprise Data Explorer v{} | Built with Streamlit & DuckDB | {}</p>
</div>
""".format(APP_VERSION, datetime.now().strftime("%Y")), unsafe_allow_html=True)
