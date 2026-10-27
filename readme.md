# ☁️ AWS Compliance Data Engineering Pipeline

<p align="center">
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python"/>
  <img src="https://img.shields.io/badge/AWS-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white" alt="AWS"/>
  <img src="https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white" alt="Apache Airflow"/>
  <img src="https://img.shields.io/badge/Amazon%20S3-569A31?style=for-the-badge&logo=amazons3&logoColor=white" alt="Amazon S3"/>
  <img src="https://img.shields.io/badge/Amazon%20Athena-232F3E?style=for-the-badge&logo=amazonaws&logoColor=white" alt="Amazon Athena"/>
</p>

An end-to-end **Data Engineering project** that processes IT asset and device compliance data using Python, Amazon S3, Apache Airflow, Amazon Athena, and AWS infrastructure.

🎯 **Goal:** Transform raw compliance data into clean, enriched, and actionable insights for IT operations, compliance monitoring, and audit reporting.

## 📌 Table of Contents

- [🏗️ Architecture](#️-architecture)
- [🛠️ Tech Stack](#️-tech-stack)
- [⚙️ Pipeline Workflow](#️-pipeline-workflow)
- [🧹 Data Processing and Business Rules](#-data-processing-and-business-rules)
- [📊 Latest KPI Results](#-latest-kpi-results)
- [🔄 Airflow DAG](#-airflow-dag)
- [📁 Project Structure](#-project-structure)
- [🚀 Key Outcomes](#-key-outcomes)
- [🔮 Future Improvements](#-future-improvements)
- [👨‍💻 Author](#-author)

## 🏗️ Architecture

```text
              📄 Raw CSV Files
                     |
                     v
              ☁️ Amazon S3
                     |
                     v
             🥉 Bronze Layer
             Raw Data → Parquet
                     |
                     v
             🥈 Silver Layer
          Cleaning + Enrichment
          Business Rules + DQ
                     |
                     v
          🗄️ AWS Glue Data Catalog
                     |
                     v
             🔎 Amazon Athena
               🥇 Gold Views
              KPI + Summaries
                     |
                     v
             📊 Excel Report
```

**Orchestration:** Apache Airflow running on Amazon EC2.

## 🛠️ Tech Stack

| Technology | Purpose |
|---|---|
| 🐍 Python | Pipeline logic and automation |
| 🐼 Pandas | Data cleaning and transformation |
| 📦 Parquet | Efficient columnar data storage |
| ☁️ Amazon S3 | Raw and processed data storage |
| 🖥️ Amazon EC2 | Cloud compute and Airflow hosting |
| 🔄 Apache Airflow | Workflow orchestration |
| 🗂️ AWS Glue Data Catalog | Table and schema metadata |
| 🔎 Amazon Athena | SQL analytics over data in S3 |
| 📊 Excel | Compliance reporting |
| 🐙 Git & GitHub | Version control and project documentation |

## ⚙️ Pipeline Workflow

The `compliance_pipeline` DAG orchestrates the following tasks:

1. 📥 **Bronze Ingestion** — Read raw files and create Parquet datasets with ingestion metadata.
2. 🧹 **Silver Transformation** — Clean, deduplicate, enrich, and classify compliance records.
3. ✅ **Silver Validation** — Validate processed data before analytics.
4. 🥇 **Gold View Refresh** — Refresh Athena views for KPI and control-level summaries.
5. 🔍 **Gold Validation** — Validate analytics outputs.
6. 📊 **Excel Report Generation** — Generate an Excel report containing KPI and control summaries.

## 🧹 Data Processing and Business Rules

The pipeline applies practical IT compliance rules:

- 🔐 Identify devices with disabled BitLocker encryption.
- 🛡️ Detect missing or outdated antivirus protection.
- 🩹 Track patch compliance and other required security controls.
- 📴 Exclude eligible returned, decommissioned, or inactive devices from actionable non-compliance KPIs.
- 🔄 Identify asset ID mismatches between source systems for investigation.
- 🚨 Preserve data-quality issues and exceptions for review by IT teams.

## 📊 Latest KPI Results

The latest recorded Athena KPI output contained **1,200 devices**.

| 📌 KPI Status | 💻 Devices | 📈 Percentage |
|---|---:|---:|
| ✅ Compliant | 151 | 12.58% |
| ⏸️ Excluded | 85 | 7.08% |
| 🚨 Non-Compliant | 964 | 80.33% |
| **📦 Total** | **1,200** | **99.99%*** |

<sub>*Percentages may not total exactly 100% because of rounding.</sub>

## 🔄 Airflow DAG

```text
📥 Bronze Ingestion
         ↓
🧹 Silver Transformation
         ↓
✅ Silver Validation
         ↓
🥇 Gold View Refresh
         ↓
🔍 Gold Validation
         ↓
📊 Excel Report Generation
```

The six tasks completed successfully in the recorded Airflow run.

## 📁 Project Structure

```text
compliant_mgmt/
├── 📂 data/
│   └── 📂 raw/
├── 📂 reports/
│   └── 📊 compliance_gold_report.xlsx
├── 📂 scripts/
│   ├── 🐍 bronze.py
│   ├── 🐍 silver.py
│   ├── 🐍 silver_validate.py
│   ├── 🐍 gold_refresh.py
│   ├── 🐍 gold_validate.py
│   ├── 🐍 gold_report.py
│   └── 🐍 run_pipeline.py
├── 📂 sql/
├── 📂 docs/
└── 📄 README.md
```

*Note: Adjust this tree to match the actual files and folders in the repository.*

## 🚀 Key Outcomes

- 🏗️ Implemented a multi-stage Bronze/Silver/Gold data architecture.
- 🔄 Orchestrated dependent data-processing tasks with Apache Airflow.
- ☁️ Stored raw and transformed datasets in Amazon S3.
- 🔎 Created Athena Gold views for compliance KPIs and control summaries.
- 📊 Generated an Excel report for compliance monitoring and investigation.
- 🧠 Applied business rules to distinguish actionable issues from records requiring exclusion or review.

## 🔮 Future Improvements

- 📩 Automate report delivery and notifications.
- 📈 Add monitoring, logging, and failure alerts.
- ⚡ Explore distributed processing with AWS Glue and Apache Spark.
- 🖼️ Add pipeline screenshots and a visual architecture diagram.

## 👨‍💻 Author

**Aman Kumar**

🎯 Aspiring Data Engineer | Python • SQL • AWS • Data Pipelines

- 🐙 GitHub: [amankumar-dev](https://github.com/amankumar-dev)
- 🌐 Portfolio: [portfolioamankumar.vercel.app](https://portfolioamankumar.vercel.app)
- 💼 LinkedIn: [aman-kumar-26738726b](https://www.linkedin.com/in/aman-kumar-26738726b/)

---

⭐ If you find this project interesting, feel free to explore the repository!
