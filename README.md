# Automated News Classification

## 1. ETL Pipeline for News Classification

- Built a scalable ETL pipeline using Selenium (web scraping), MongoDB (transformation/storage), and GCP (Composer, Storage) to aggregate data from Kaggle, Twitter (limited), and Google News APIs. Automated workflows via Airflow DAGs for scheduled updates.

## 2. Pivot Due to API Limitations

- Initially aimed to use Twitter API for training data but scaled back due to rate limits. Shifted focus to Google News API, deploying a DAG to fetch articles biweekly, and trained a Logistic Regression model (10K samples) to classify fake/real news.

## 3. End-to-End Workflow with ML Integration

- Processed data in PySpark, stored results in MongoDB, and generated predictions (50 test samples). Project laid groundwork for distributed systems, highlighting adaptability in tooling (e.g., GCP services) and team collaboration.
