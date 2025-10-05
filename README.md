# 🛒 E-Commerce Analytics ETL Pipeline & Dashboard

## 📊 Project Overview
A complete end-to-end data pipeline that generates synthetic e-commerce data, processes it through an automated ETL pipeline, and delivers interactive business insights through a Power BI dashboard.

![Dashboard Preview](screenshots/dashboard.png)

## 🏗️ System Architecture
Data Generation → ETL Processing → Data Warehouse → Power BI Dashboard

## 📁 Project Structure
ecommerce-analytics-pipeline/
├── generate_data.py # Synthetic e-commerce data generation
├── etl_pipeline.py # Complete ETL processing pipeline
├── export_for_powerbi.py # Data export utility
├── run_pipeline.py # Master automation script
├── run_pipeline.bat # One-click execution (Windows)
├── requirements.txt # Python dependencies
├── README.md # Project documentation
└── screenshots/ # Dashboard preview images
├── dashboard.png


## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- MySQL Server
- Power BI Desktop (for visualization)

### Installation
1. Clone this repository
   ```bash
   git clone https://github.com/ramanantheking1/ecommerce-analytics-pipeline.git
   cd ecommerce-analytics-pipeline

Install Python dependencies
```bash
pip install -r requirements.txt
```
Set up MySQL databases
```bash
CREATE DATABASE ecommerce_source;
CREATE DATABASE ecommerce_dw;
```
Usage

1.Run the complete pipeline:
```bash
python run_pipeline.py

```
or double-click run_pipeline.bat (Windows)

2.Connect Power BI to MySQL:

Server: localhost
Database: ecommerce_dw
Username: root (Password: leave empty)

3.Refresh Power BI to see updated data

🛠️ Technologies Used
* Python (Pandas, SQLAlchemy, Faker)

* MySQL Database Management

* Power BI Data Visualization

* ETL Pipeline Design & Implementation

* Star Schema Data Modeling

📈 Features

🔄 Automated Data Pipeline
Synthetic Data Generation: Realistic e-commerce data

Complete ETL Process: Extract, Transform, Load with error handling

Data Warehousing: Optimized star schema design

One-Click Automation: Full pipeline execution

📊 Power BI Dashboard
Key Performance Indicators: Revenue, orders, average order value

Interactive Visualizations: Trend analysis, customer segmentation

Dynamic Filtering: Date ranges, customer segments, categories

Professional Design: Clean, business-ready dashboard

💼 Business Insights
Customer Analytics: Segmentation, lifetime value

Sales Performance: Revenue trends, product performance

Operational Metrics: Order volume, profit analysis
