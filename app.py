import os
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

import streamlit as st
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, initcap, lower, trim

# ==========================================
# 1. PAGE CONFIGURATION
# ==========================================
st.set_page_config(page_title="Data Quality Engine", page_icon="📊", layout="wide")
st.title("📊 Universal Big Data Quality Scorer")
st.write("Upload any raw dataset, map your columns, and evaluate its health across five core dimensions.")

# ==========================================
# 2. FILE UPLOADER
# ==========================================
uploaded_file = st.file_uploader("Upload Raw Data (CSV)", type=["csv"])

if uploaded_file is not None:
    temp_path = "temp_dataset.csv"
    with open(temp_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
        
    # Initialize Local Spark Cluster
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("UniversalDataQuality") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    # Read Data
    df = spark.read.csv(temp_path, header=True, inferSchema=True)
    columns = df.columns
    total_records = df.count()
    
    if total_records == 0:
        st.error("The uploaded CSV is empty.")
        spark.stop()
        st.stop()

    # ==========================================
    # 3. DYNAMIC COLUMN MAPPING (SIDEBAR)
    # ==========================================
    with st.sidebar:
        st.header("⚙️ Rule Configuration")
        st.write("Map your dataset's columns to the quality engine rules.")
        
        # Form prevents the engine from running until the user clicks the button
        with st.form("mapping_form"):
            id_col = st.selectbox("1. Uniqueness (Primary Key)", columns, help="Column that should have no duplicates (e.g., ID).")
            comp_col = st.selectbox("2. Completeness (Critical Field)", columns, help="Column that cannot contain null/empty values.")
            
            st.divider()
            acc_col = st.selectbox("3. Accuracy (Numeric Field)", columns, help="Column to check for valid numerical bounds.")
            col1, col2 = st.columns(2)
            with col1:
                acc_min = st.number_input("Min Valid Value", value=0.0)
            with col2:
                acc_max = st.number_input("Max Valid Value", value=100.0)
            
            st.divider()
            val_col = st.selectbox("4. Validity (Text Pattern)", columns, help="Column to check against a Regex pattern.")
            val_regex = st.text_input("Regex Pattern (Default: Email)", value=r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$")
            
            st.divider()
            cons_col = st.selectbox("5. Consistency (Categorical)", columns, help="Text column to standardize casing (e.g., status, category).")
            
            run_pipeline = st.form_submit_button("🚀 Run Quality Engine")

    # ==========================================
    # 4. PYSPARK PROCESSING PIPELINE
    # ==========================================
    if run_pipeline:
        with st.spinner('Processing Pipeline Active...'):
            
            # --- A. QUALITY SCORING ENGINE ---
            unique_records = df.dropDuplicates([id_col]).count()
            uniqueness_score = (unique_records / total_records) * 100

            completeness_pass = df.filter(col(comp_col).isNotNull()).count()
            completeness_score = (completeness_pass / total_records) * 100

            accuracy_pass = df.filter(col(acc_col).between(acc_min, acc_max)).count()
            accuracy_score = (accuracy_pass / total_records) * 100

            validity_pass = df.filter(col(val_col).rlike(val_regex)).count()
            validity_score = (validity_pass / total_records) * 100

            clean_format = initcap(trim(lower(col(cons_col))))
            consistency_pass = df.filter(col(cons_col) == clean_format).count()
            consistency_score = (consistency_pass / total_records) * 100

            scores = {
                "Dimension": ["Uniqueness", "Completeness", "Accuracy", "Validity", "Consistency"],
                "Score": [uniqueness_score, completeness_score, accuracy_score, validity_score, consistency_score]
            }
            results_df = pd.DataFrame(scores)

            # --- B. DEEP ANALYSIS ENGINE ---
            stats_df = df.describe([acc_col]).toPandas()
            
            # --- C. AUTOMATED REMEDIATION ENGINE ---
            clean_df = df.dropDuplicates([id_col])
            clean_df = clean_df.filter(col(comp_col).isNotNull())
            clean_df = clean_df.filter(col(acc_col).between(acc_min, acc_max))
            clean_df = clean_df.filter(col(val_col).rlike(val_regex))
            clean_df = clean_df.withColumn(cons_col, clean_format)
            
            clean_records_count = clean_df.count()
            clean_pd_df = clean_df.toPandas()
            clean_csv = clean_pd_df.to_csv(index=False).encode('utf-8')

        # ==========================================
        # 5. TABBED FRONTEND DASHBOARD
        # ==========================================
        tab1, tab2, tab3 = st.tabs(["📊 Health Dashboard", "🔍 Deep Analysis", "🛠️ Data Cleaning & Export"])
        
        with tab1:
            overall_score = results_df["Score"].mean()
            if overall_score >= 90:
                st.success(f"Overall Quality Score: {overall_score:.2f}% (Excellent)")
            elif overall_score >= 75:
                st.warning(f"Overall Quality Score: {overall_score:.2f}% (Needs Review)")
            else:
                st.error(f"Overall Quality Score: {overall_score:.2f}% (Poor Quality)")
                
            col1, col2 = st.columns(2)
            with col1:
                st.dataframe(results_df.style.format({"Score": "{:.2f}%"}), use_container_width=True, hide_index=True)
            with col2:
                st.bar_chart(results_df.set_index("Dimension"))
                
        with tab2:
            st.subheader("Statistical Profiling")
            st.write(f"Automatically generated metadata for your target numeric column: `{acc_col}`")
            st.dataframe(stats_df, use_container_width=True, hide_index=True)
            st.write(f"**Total Raw Rows:** {total_records} | **Total Raw Columns:** {len(columns)}")
            
        with tab3:
            st.subheader("Automated Remediation")
            col_a, col_b = st.columns(2)
            col_a.metric("Original Rows", total_records)
            col_b.metric("Cleaned Rows Retained", clean_records_count, delta=clean_records_count - total_records)
            
            st.divider()
            st.write("**Preview of Cleaned Data:**")
            st.dataframe(clean_pd_df.head(50), use_container_width=True, hide_index=True)
            
            st.download_button("📥 Download Cleaned Dataset (CSV)", data=clean_csv, file_name="cleaned_data.csv", mime="text/csv")
    
    # Shut down cluster if the user hasn't hit run yet
    else:
        spark.stop()