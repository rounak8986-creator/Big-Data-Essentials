# 📊 Universal Big Data Quality Scorer & Remediation Engine

![Python](https://img.shields.io/badge/Python-3.12%2B-blue?logo=python&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-PySpark%203.5.1-E25A1C?logo=apachespark&logoColor=white)
![Streamlit](https://img.shields.io/badge/Frontend-Streamlit-FF4B4B?logo=streamlit&logoColor=white)
![Status](https://img.shields.io/badge/Status-Completed-success)

An end-to-end, **schema-agnostic Big Data Quality Scoring and Automated Remediation framework** built using **Apache Spark (PySpark)** and **Streamlit**. 

Designed to solve the *"Garbage In, Garbage Out" (GIGO)* bottleneck in data engineering and machine learning pipelines, this application dynamically audits any raw CSV dataset across **five core data quality dimensions**, performs descriptive statistical profiling, and executes automated in-memory transformations to export clean, production-ready datasets.

---

## ✨ Key Features

* **🔄 Dynamic Schema Mapping (Schema-Agnostic):** No hardcoded column names. Upload any CSV dataset (Educational, Financial, Healthcare, E-Commerce) and map columns dynamically via an interactive sidebar UI.
* **⚡ Distributed In-Memory Processing:** Powered by Apache Spark (`local[*]`), utilizing all available CPU cores in parallel with Catalyst query optimization and lazy evaluation.
* **📏 5-Dimension Quality Auditing:** Mathematically quantifies dataset health across **Uniqueness, Completeness, Accuracy, Validity, and Consistency**.
* **📈 Deep Statistical Profiling:** Automatically computes count, mean, standard deviation, min, and max distributions for target numeric columns.
* **🛠️ Automated Data Remediation:** 
  * *Destructive Filtering:* Removes duplicate records, null critical fields, out-of-bounds numerical outliers, and broken regex patterns.
  * *Constructive Transformation:* Standardizes inconsistent text casing and trims whitespace automatically (`initcap(trim(lower()))`).
* **📥 One-Click Clean Export:** Preview remediated records in real time and download the sanitized dataset directly as a CSV.

---

## 📐 Methodology & Quality Dimensions

The PySpark engine evaluates dataset health ($H$) by computing individual dimension scores ($S_i$) and calculating the unweighted arithmetic mean:

| Dimension | Definition | PySpark Rule Applied | Mathematical Formula |
| :--- | :--- | :--- | :--- |
| **1. Uniqueness** | Absence of duplicate records based on the primary key. | `df.dropDuplicates([id_col])` | $S_{\text{unique}} = \left(\frac{N_{\text{distinct}}}{N_{\text{total}}}\right) \times 100$ |
| **2. Completeness** | Presence of non-null values in critical mandatory fields. | `col(comp_col).isNotNull()` | $S_{\text{complete}} = \left(\frac{N_{\text{non-null}}}{N_{\text{total}}}\right) \times 100$ |
| **3. Accuracy** | Adherence of numeric values to valid domain bounds $[min, max]$. | `col(acc_col).between(min, max)` | $S_{\text{accurate}} = \left(\frac{N_{\text{in-bounds}}}{N_{\text{total}}}\right) \times 100$ |
| **4. Validity** | Conformance of text strings to structural standards (e.g., Email Regex). | `col(val_col).rlike(regex)` | $S_{\text{valid}} = \left(\frac{N_{\text{regex-match}}}{N_{\text{total}}}\right) \times 100$ |
| **5. Consistency** | Uniformity of categorical casing and formatting across records. | `col == initcap(trim(lower(col)))` | $S_{\text{consistent}} = \left(\frac{N_{\text{standardized}}}{N_{\text{total}}}\right) \times 100$ |

### Health Classification Thresholds
* 🟢 **90.0% – 100.0%:** Excellent Quality (Ready for ML/Analytics Ingestion)
* 🟡 **75.0% – 89.9%:** Needs Review (Moderate Anomalies Detected)
* 🔴 **Below 75.0%:** Poor Quality (Halt Ingestion & Remediate)

---

## 🛠️ Technology Stack

* **Distributed Backend Engine:** Apache Spark (`pyspark==3.5.1`), Spark SQL Functions
* **Interactive Frontend UI:** Streamlit (`streamlit`)
* **Data Bridge & Export Layer:** Pandas (`pandas`)
* **Pattern Matching:** Regular Expressions (`Regex`)
* **Environment & Language:** Python 3.12+, Java JDK 11/17, Visual Studio Code

---

## 📂 Project Structure

```text
Big-Data-Essentials/
│
├── app.py                                  # Main integrated Streamlit + PySpark application
├── generate_data.py                        # Synthetic big data generator script with error injection
├── requirements.txt                        # Python package dependencies
├── README.md                               # Project documentation
│
└── raw_data/                               # Sample datasets for testing
    ├── student_dataset.csv                 # Small sample dataset
    ├── university_student_records_1050.csv # 1,050-row university dataset with anomalies
    └── campus_placement_records_525.csv    # 525-row placement dataset (alternate schema)
```

---

## 🚀 Installation & Local Setup

### Prerequisites
* **Python 3.10 – 3.12+** installed
* **Java Development Kit (JDK 8, 11, or 17)** installed and `JAVA_HOME` configured (required by Apache Spark)

### 1. Clone the Repository
```bash
git clone [https://github.com/rounak8986-creator/Big-Data-Essentials.git](https://github.com/rounak8986-creator/Big-Data-Essentials.git)
cd Big-Data-Essentials
```

### 2. Create and Activate a Virtual Environment
```cmd
python -m venv env
.\env\Scripts\activate
```

### 3. Install Dependencies
```cmd
pip install -r requirements.txt
```

### 4. Launch the Application
```cmd
streamlit run app.py
```
The dashboard will automatically open in your default browser at `http://localhost:8501`.

---

## 🧪 How to Usage / Test the Pipeline

1. **Upload Dataset:** Click **Browse files** and select any `.csv` file (e.g., `university_student_records_1050.csv`).
2. **Map Columns (Left Sidebar):**
   * Select your **Primary Key** column for *Uniqueness* (e.g., `Roll_Number`).
   * Select your **Critical Field** for *Completeness* (e.g., `Institute_Email`).
   * Select your **Numeric Field** and define `[Min, Max]` bounds for *Accuracy* (e.g., `Attendance_Pct` between `0.0` and `100.0`).
   * Select your **Text Pattern** column and Regex for *Validity* (e.g., `Institute_Email`).
   * Select your **Categorical** column for *Consistency* (e.g., `Enrollment_Status`).
3. **Execute Pipeline:** Click **🚀 Run Quality Engine**.
4. **Explore & Export:**
   * **Tab 1 (📊 Health Dashboard):** View overall dataset health, dimension scores, and bar chart distributions.
   * **Tab 2 (🔍 Deep Analysis):** Inspect statistical profiles (`count`, `mean`, `stddev`, `min`, `max`).
   * **Tab 3 (🛠️ Data Cleaning & Export):** Compare original vs. retained row counts, preview sanitized records, and click **📥 Download Cleaned Dataset (CSV)**.

---

## 👨‍💻 Author

* **GitHub:** [@rounak8986-creator](https://github.com/rounak8986-creator)
* **Course / Domain:** B.Tech Computer Science & Engineering — Big Data Essentials
