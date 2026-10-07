from datetime import datetime
import os
import time
import re
import hashlib
import json
import pandas as pd
import numpy as np
import plotly.express as px
import sqlite3

# 1. இயக்க முறைமை மற்றும் சேமிப்பகப் பாதைகள் அமைப்பு
if os.name == 'posix' and os.path.exists('/storage/emulated/0/Download/'):
    DOWNLOAD_FOLDER = "/storage/emulated/0/Download/"
else:
    DOWNLOAD_FOLDER = os.path.join(os.path.expanduser("~"), "Downloads")
    if not os.path.exists(DOWNLOAD_FOLDER):
        os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
        if not DOWNLOAD_FOLDER.endswith('/') and not DOWNLOAD_FOLDER.endswith('\\'):
            DOWNLOAD_FOLDER += os.sep

DB_NAME = os.path.join(DOWNLOAD_FOLDER, "enterprise_analytics_engine.db")
QUARANTINE_FOLDER = os.path.join(DOWNLOAD_FOLDER, "error_quarantine/")
PROCESSED_FOLDER = os.path.join(DOWNLOAD_FOLDER, "processed_archives/")
BACKUP_FOLDER = os.path.join(DOWNLOAD_FOLDER, "csv_backups/")
REPORTS_FOLDER = os.path.join(DOWNLOAD_FOLDER, "generated_reports/")
LOG_FILE_PATH = os.path.join(DOWNLOAD_FOLDER, "system_execution.log")

for folder in [QUARANTINE_FOLDER, PROCESSED_FOLDER, BACKUP_FOLDER, REPORTS_FOLDER]:
    if not os.path.exists(folder):
        os.makedirs(folder, exist_ok=True)

def log_activity(message):
    """கணினி கண்காணிப்பு மற்றும் நிகழ்வுப் பதிவு"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_message = f"[{timestamp}] {message}"
    print(log_message)
    
    try:
        if os.path.exists(LOG_FILE_PATH) and os.path.getsize(LOG_FILE_PATH) > 5 * 1024 * 1024:
            with open(LOG_FILE_PATH, "w", encoding="utf-8") as log_file:
                log_file.write(f"[{timestamp}] System Log Reset: Size limit reached.\n")
        
        with open(LOG_FILE_PATH, "a", encoding="utf-8") as log_file:
            log_file.write(log_message + "\n")
    except Exception as e:
        print(f"Log Writing Error: {e}")

def generate_final_executive_directive(automated_decisions, anomalies_count):
    pos_count = sum(1 for d in automated_decisions if "POSITIVE TREND" in d)
    neg_count = sum(1 for d in automated_decisions if "NEGATIVE TREND" in d)
    
    if anomalies_count > 0:
        return "🎯 FINAL EXECUTIVE DIRECTIVE: CRITICAL ANOMALIES DETECTED. IMMEDIATE DATA AUDIT REQUIRED BEFORE TAKING FURTHER ACTION."
    elif pos_count > neg_count:
        return "🎯 FINAL EXECUTIVE DIRECTIVE: STRONG GROWTH DETECTED. IMMEDIATELY AGGRESSIVELY SCALE UP OPERATIONS AND INVENTORY TO MAXIMIZE PROFIT."
    elif neg_count > pos_count:
        return "🎯 FINAL EXECUTIVE DIRECTIVE: PERFORMANCE DROP IDENTIFIED. PAUSE EXPANSION AND IMPLEMENT IMMEDIATE OPERATIONAL OPTIMIZATION."
    else:
        return "🎯 FINAL EXECUTIVE DIRECTIVE: METRICS ARE STABLE. MAINTAIN CURRENT STRATEGY AND MONITOR DAILY TRENDS."

def get_file_hash(data_source):
    hasher = hashlib.sha256()
    try:
        if isinstance(data_source, str) and os.path.exists(data_source):
            with open(data_source, 'rb') as f:
                buf = f.read()
                hasher.update(buf)
        else:
            hasher.update(str(data_source).encode('utf-8'))
        return hasher.hexdigest()
    except Exception:
        return None

log_activity("System Initialization: Enterprise Pipeline with Automated Excel Cleaning started.")

def run_enterprise_pipeline():
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS file_tracking_ledger (
                file_hash TEXT PRIMARY KEY,
                file_name TEXT,
                processed_time TEXT
            )
        """)
        conn.commit()
        
        cursor.execute("SELECT file_hash FROM file_tracking_ledger;")
        processed_hashes = {row[0] for row in cursor.fetchall()}
        
        log_activity("Pipeline Watcher: Scanning directory for datasets.")
        for file_name in os.listdir(DOWNLOAD_FOLDER):
            file_path = os.path.join(DOWNLOAD_FOLDER, file_name)
            
            if os.path.isdir(file_path):
                continue
            try:
                if file_name.endswith((".csv", ".xlsx", ".xls", ".json")) and not file_name.startswith("~"):
                    file_hash = get_file_hash(file_path)
                    if file_hash in processed_hashes:
                        continue
                    
                    file_start_time = time.time()
                    base_name = os.path.splitext(file_name)[0]
                    table_name = re.sub(r'[^a-zA-Z0-9_]', '_', base_name).lower()
                    
                    if not table_name or table_name[0].isdigit():
                        table_name = "tbl_" + table_name
                        
                    log_activity(f"Dataset Processing Initiated: {file_name}")
                    
                    # 1 & 2. தரவு சேகரிப்பு மற்றும் வாசித்தல் (Robust Reader with Engine Fallback)
                    df = None
                    try:
                        if file_name.endswith(".csv"):
                            try:
                                df = pd.read_csv(file_path, encoding="utf-8", on_bad_lines="skip", engine="python")
                            except UnicodeDecodeError:
                                df = pd.read_csv(file_path, encoding="latin-1", on_bad_lines="skip", engine="python")
                        elif file_name.endswith(".xlsx"):
                            df = pd.read_excel(file_path, engine="openpyxl")
                        elif file_name.endswith(".xls"):
                            try:
                                df = pd.read_excel(file_path, engine="xlrd")
                            except Exception:
                                # ஒருவேளை .xls பெயரில் உள்ள கோப்பு உண்மையில் .xlsx அல்லது வேறு ஃபார்மட்டாக இருந்தால் openpyxl மூலம் முயற்சிக்க
                                df = pd.read_excel(file_path, engine="openpyxl")
                        elif file_name.endswith(".json"):
                            df = pd.read_json(file_path)
                    except Exception as read_err:
                        log_activity(f"Error Quarantine: Moving failed file {file_name} -> {read_err}")
                        quarantine_dest = os.path.join(QUARANTINE_FOLDER, file_name)
                        if os.path.exists(file_path):
                            os.rename(file_path, quarantine_dest)
                        continue
                        
                    if df is None or df.empty:
                        log_activity(f"Warning: Dataset '{file_name}' is empty.")
                        continue
                        
                    
                    # 3 & 4. தரவுத் தூய்மைப்படுத்தல் மற்றும் சீரமைப்பு (Advanced Cleaning)
                    
                    df.columns = [re.sub(r'[^a-zA-Z0-9_]', '_', str(col).strip().lower()) for col in df.columns]
                    df = df.dropna(how='all').dropna(axis=1, how='all').drop_duplicates()
                    
                    for col in df.select_dtypes(include=['object', 'string']).columns:
                        df[col] = df[col].astype(str).str.strip()
                        df[col] = df[col].replace(to_replace=[r'^\s*$', r'nan', r'None', r'NULL', r'null'], value=np.nan, regex=True)
                    
                    # பிரத்யேக எக்செல் துப்புரவு (Category & Date திருத்தம்)
                    if 'category' in df.columns:
                        df['category'] = df['category'].fillna('General')
                        df['category'] = df['category'].replace('Unknown', 'General')
                        
                    if 'upload_date' in df.columns:
                        df['upload_date'] = pd.to_datetime(df['upload_date'], errors='coerce')
                        median_date = df['upload_date'].median()
                        if pd.notna(median_date):
                            df['upload_date'] = df['upload_date'].fillna(median_date)

                    for col in df.select_dtypes(include=['number']).columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                        median_val = df[col].median()
                        df[col] = df[col].fillna(median_val if pd.notna(median_val) else 0)
                        
                    log_activity("Data Cleansing & Advanced Normalization Completed.")
                    
                    
                    # 5. புதிய அம்சப் பொறியியல் (Feature Engineering)
                    
                    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
                    if len(numeric_cols) >= 2:
                        df['engineered_sum_feature'] = df[numeric_cols[0]] + df[numeric_cols[1]]
                        df['engineered_mean_feature'] = df[numeric_cols].mean(axis=1)
                        log_activity("Feature Engineering: New derived metrics generated.")
                        
                    df.to_sql(table_name, conn, if_exists="replace", index=False)
                    backup_csv_path = os.path.join(BACKUP_FOLDER, f"{table_name}_backup.csv")
                    df.to_csv(backup_csv_path, index=False, encoding="utf-8")
                    
                    # ========================================================
                    # 6. புள்ளிவிவர ஆய்வு மற்றும் தானியங்கி முடிவு எடுக்கும் அமைப்பு
                    # ========================================================
                    statistical_insights = []
                    automated_decisions = []
                    anomalies_count = 0
                    numeric_df = df.select_dtypes(include=['number'])
                    
                    if not numeric_df.empty:
                        stats_df = numeric_df.describe().reset_index()
                        stats_df.to_sql(f"{table_name}_statistics", conn, if_exists="replace", index=False)
                        
                        for col in numeric_df.columns:
                            max_val = numeric_df[col].max()
                            min_val = numeric_df[col].min()
                            mean_val = numeric_df[col].mean()
                            std_val = numeric_df[col].std()
                            sum_val = numeric_df[col].sum()
                            
                            metric_line = f"[{col.upper()}] -> Max: {max_val} | Min: {min_val} | Sum: {sum_val} | Mean: {round(mean_val, 4)} | StdDev: {round(std_val, 4)}"
                            statistical_insights.append(metric_line)
                            
                        for col in numeric_df.columns:
                            series = numeric_df[col]
                            if len(series) >= 2:
                                growth_rate = ((series.iloc[-1] - series.iloc[0]) / (series.iloc[0] if series.iloc[0] != 0 else 1)) * 100
                                if growth_rate > 10:
                                    automated_decisions.append(f"🟢 POSITIVE TREND in '{col}': Growth observed at {round(growth_rate, 2)}%. DECISION: Scale up production/efforts for this metric.")
                                elif growth_rate < -10:
                                    automated_decisions.append(f"🔴 NEGATIVE TREND in '{col}': Drop observed at {round(growth_rate, 2)}%. DECISION: Immediate intervention and optimization required.")
                                else:
                                    automated_decisions.append(f"🟡 STABLE TREND in '{col}': Performance is consistent. DECISION: Maintain current baseline strategy.")
                                    
                        try:
                            Q1 = numeric_df.quantile(0.25)
                            Q3 = numeric_df.quantile(0.75)
                            IQR = Q3 - Q1
                            lower_bound = Q1 - 1.5 * IQR
                            upper_bound = Q3 + 1.5 * IQR
                            
                            outlier_mask = ((numeric_df < lower_bound) | (numeric_df > upper_bound)).any(axis=1)
                            outlier_records = df[outlier_mask]
                            
                            if not outlier_records.empty:
                                outlier_records.to_sql(f"{table_name}_anomalies", conn, if_exists="replace", index=False)
                                anomalies_count = len(outlier_records)
                                automated_decisions.append(f"⚠️ ANOMALY ALERT: Found {anomalies_count} outlier rows. DECISION: Inspect quarantined data anomalies immediately to protect model accuracy.")
                                log_activity(f"Anomaly Detection (IQR): Found {anomalies_count} anomalies.")
                        except Exception as ml_err:
                            log_activity(f"Anomaly Detection Error: {ml_err}")
                            
                    final_directive = generate_final_executive_directive(automated_decisions, anomalies_count)
                    
                    # ========================================================
                    # 7. டைனமிக் காட்சிப்படுத்தல் (Dynamic Chart Selection Logic)
                    # ========================================================
                    x_axis_col, y_axis_col = None, None
                    if len(df.columns) >= 2:
                        for col in ['upload_date', 'date', 'month', 'year', 'id', 'name', 'timestamp', 'day', 'index', 'code', 'video_id']:
                            if col in df.columns:
                                x_axis_col = col
                                break
                        if not x_axis_col: 
                            x_axis_col = df.columns[0]
                            
                        for col in ['views', 'revenue_inr', 'likes', 'marks', 'amount', 'revenue', 'value', 'price', 'total', 'count', 'score', 'sales', 'rate']:
                            if col in df.columns:
                                y_axis_col = col
                                break
                        if not y_axis_col and not numeric_df.empty:
                            y_axis_col = numeric_df.columns[0]
                            
                        if x_axis_col and y_axis_col:
                            is_date_related = any(term in x_axis_col.lower() for term in ['date', 'time', 'year', 'month', 'day'])
                            is_categorical = df[x_axis_col].dtype == 'object' or df[x_axis_col].dtype == 'string'
                            
                            if len(numeric_cols) > 1 and is_date_related:
                                fig = px.line(df, x=x_axis_col, y=numeric_cols[:4], title=f"Complete Multi-Metric Analytics: {table_name.upper()}", template="plotly_white", markers=True)
                            elif is_date_related:
                                fig = px.line(df, x=x_axis_col, y=y_axis_col, title=f"Time-Series Analytics: {table_name.upper()}", template="plotly_white", markers=True)
                            elif is_categorical and len(df[x_axis_col].unique()) <= 20:
                                fig = px.bar(df, x=x_axis_col, y=y_axis_col, title=f"Categorical Distribution: {table_name.upper()}", template="plotly_white", text=y_axis_col)
                                fig.update_traces(textposition='outside')
                            else:
                                fig = px.scatter(df, x=x_axis_col, y=y_axis_col, title=f"Correlation Analytics: {table_name.upper()}", template="plotly_white")
                                
                            fig.update_layout(title_font=dict(size=18, family='Arial'), hovermode="x unified", xaxis_title=x_axis_col.title(), yaxis_title="Metrics Value")
                            chart_file_path = os.path.join(DOWNLOAD_FOLDER, f"{table_name}_visualization.html")
                            fig.write_html(chart_file_path)
                            log_activity(f"Visualization Generated: {chart_file_path}")
                            
                    execution_duration = round((time.time() - file_start_time) * 1000, 2)
                    
                    
                    # 8. அறிக்கைகள் தயாரிப்பு (JSON மற்றும் Markdown)
                    
                    report_payload = {
                        "file_name": file_name,
                        "table_name": table_name,
                        "total_rows": len(df),
                        "total_columns": len(df.columns),
                        "anomalies_detected": anomalies_count,
                        "statistical_summary": statistical_insights,
                        "automated_decisions": automated_decisions,
                        "final_executive_directive": final_directive,
                        "processing_time_ms": execution_duration,
                        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    
                    json_report_path = os.path.join(REPORTS_FOLDER, f"{table_name}_report.json")
                    with open(json_report_path, "w", encoding="utf-8") as jf:
                        json.dump(report_payload, jf, indent=4, ensure_ascii=False)
                        
                    insights_markdown = "\n".join([f"- {item}" for item in statistical_insights])
                    decisions_markdown = "\n".join([f"- {dec}" for dec in automated_decisions])
                    
                    md_report_path = os.path.join(REPORTS_FOLDER, f"{table_name}_report.md")
                    markdown_content = (
                        f"# Enterprise Data Science & Automated Decision Report\n"
                        f"| Parameter | Value |\n"
                        f"| :--- | :--- |\n"
                        f"| **Source File** | `{file_name}` |\n"
                        f"| **Database Table** | `{table_name}` |\n"
                        f"| **Total Rows** | `{len(df)}` |\n"
                        f"| **Total Columns** | `{len(df.columns)}` |\n"
                        f"| **Outliers Found** | `{anomalies_count}` |\n"
                        f"| **Execution Time** | `{execution_duration} ms` |\n\n"
                        f"### 🤖 Automated Decisions & Strategic Actions:\n{decisions_markdown}\n\n"
                        f"---\n"
                        f"### 🚀 ONE-LINE FINAL ACTION PLAN:\n"
                        f"> **`{final_directive}`**\n\n"
                        f"### Statistical Insights:\n{insights_markdown}\n"
                    )
                    
                    with open(md_report_path, "w", encoding="utf-8") as mdf:
                        mdf.write(markdown_content)
                        
                    log_activity("Reporting Complete: Automated Decisions and Final Directive generated successfully.")
                    
                    cursor.execute("INSERT OR REPLACE INTO file_tracking_ledger VALUES (?, ?, ?)", 
                                   (file_hash, file_name, report_payload['timestamp']))
                    conn.commit()
                    
                    processed_dest = os.path.join(PROCESSED_FOLDER, file_name)
                    if os.path.exists(file_path):
                        os.rename(file_path, processed_dest)
                        log_activity("File Archived Successfully.")
                        
            except Exception as file_loop_err:
                log_activity(f"Runtime Exception: {file_loop_err}")
                continue
              
        
        # 9. ஆட்டோ கிளீன் (Database & Storage Auto-Cleanup Engine)
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        all_tables = [row[0] for row in cursor.fetchall()]
        
        for tbl in all_tables:
            if tbl != "file_tracking_ledger":
                cursor.execute(f"DROP TABLE IF EXISTS {tbl};")
        conn.commit()
        
        cursor.execute("VACUUM;")
        conn.commit()
        log_activity("Database Auto-Cleanup & VACUUM executed successfully to free up storage.")

        current_time = time.time()
        for filename in os.listdir(DOWNLOAD_FOLDER):
            if filename.endswith("_visualization.html"):
                full_path = os.path.join(DOWNLOAD_FOLDER, filename)
                if os.path.isfile(full_path) and (current_time - os.path.getmtime(full_path) > 7 * 86400):
                    os.remove(full_path)
                    
        conn.close()
    except Exception as global_err:
        log_activity(f"Critical System Error: {global_err}")

if __name__ == "__main__":
    while True:
        run_enterprise_pipeline()
        time.sleep(30)
