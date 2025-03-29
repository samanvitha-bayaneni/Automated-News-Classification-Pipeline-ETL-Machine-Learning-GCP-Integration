from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from datetime import datetime, timedelta, timezone
from gnews import GNews
import pandas as pd
import os
import logging
from airflow.models import Variable
import tempfile
import tenacity

# after creating a cloud composer instance, install the gnews package in the environment before uploading this DAG
# or else the DAG won't be able to run

log = logging.getLogger(__name__)

@tenacity.retry(stop=tenacity.stop_after_attempt(3), wait=tenacity.wait_exponential(multiplier=1, min=4, max=10))
def fetch_news(start_date_str, end_date_str, google_news):
    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

        google_news.start_date = (start_date.year, start_date.month, start_date.day)
        google_news.end_date = (end_date.year, end_date.month, end_date.day)
        news = google_news.get_news("news")
        return news
    except ValueError as ve:
        log.error(f"Date parsing error: {ve}")
        return []
    except Exception as e:
        log.error(f"An unexpected error occurred: {e}")
        return []


def fetch_and_save_news(**kwargs):
    google_news = GNews() # Create GNews instance inside the function
    all_news = []
    execution_date = kwargs['execution_date']
    current_date = execution_date
    end_date = current_date
    start_date = current_date - timedelta(days=1)
    max_unfiltered_count = int(Variable.get("max_unfiltered_count", default_var=100))
    days_to_go_back = int(Variable.get("days_to_go_back", default_var=2))
    gcs_bucket = Variable.get("gcs_bucket_name", default_var="msds-697-dag-test")

    def filter_us_outlets(news_list):
        us_outlets = [
            "cnn", "fox news", "cbs news", "nbc news", "abc news", "reuters",
            "associated press", "bloomberg", "wall street journal", "new york times",
            "washington post", "usa today", "los angeles times", "npr", "pbs"
        ]
        us_outlets_lower = [outlet.lower() for outlet in us_outlets]
        filtered_news = []
        for article in news_list:
            if 'publisher' in article and 'title' in article['publisher']:
                publisher_name_lower = article['publisher']['title'].lower()
                if any(outlet in publisher_name_lower for outlet in us_outlets_lower):
                    filtered_news.append(article)
        return filtered_news

    def convert_to_dataframe(news_list):
        if not news_list:
            log.info("No news articles to convert.")
            return None

        data = []
        for article in news_list:
            data.append({
                'title': article.get('title', ''),
                'date': article.get('published date', ''),
                'source': article['publisher'].get('title', '') if 'publisher' in article and isinstance(article['publisher'], dict) else ''
            })

        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        if df.empty:
            log.info("DataFrame is empty after processing.")
            return None
        return df

    while len(all_news) < max_unfiltered_count and days_to_go_back >= 0:
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

        log.info(f"Fetching news from {start_date_str} to {end_date_str}...")
        chunk_news = fetch_news(start_date_str, end_date_str, google_news)

        if chunk_news:
            all_news.extend(chunk_news)
            log.info(f"Fetched {len(chunk_news)} articles.  Total (unfiltered) so far: {len(all_news)}")

        end_date = start_date - timedelta(seconds=1)
        start_date = end_date - timedelta(days=1)

        days_to_go_back -= 1

    filtered_news = filter_us_outlets(all_news)
    log.info(f"\nNumber of articles after filtering for US outlets: {len(filtered_news)}")

    news_df = convert_to_dataframe(filtered_news)

    if news_df is not None:
        log.info("\nFirst 5 rows of the DataFrame:")
        log.info(news_df.head())
        fd, file_path = tempfile.mkstemp(suffix=".csv", prefix="us_news_articles_", dir='/tmp')
        os.close(fd)
        news_df.to_csv(file_path, index=False)
        log.info(f'saved as {file_path}')

        execution_date_str = execution_date.strftime("%Y-%m-%d")
        file_name = os.path.basename(file_path)
        gcs_file_path = os.path.join("news_data", execution_date_str, file_name)

        ti = kwargs['ti']
        try:
            ti.xcom_push(key='file_path', value=file_path)
            ti.xcom_push(key='gcs_file_path', value=gcs_file_path) 
        except Exception as e:
            log.error(f"XCom push failed: {e}")
            return None
        return file_path 
    else:
        log.info("No DataFrame created.")
        return None



def delete_local_file(**kwargs):
    ti = kwargs['ti']
    file_path = ti.xcom_pull(task_ids='fetch_news_and_save', key='file_path')
    if file_path and os.path.exists(file_path):
        try:
            os.remove(file_path)
            log.info(f"Deleted local file: {file_path}")
        except OSError as e:
            log.error(f"Error deleting file {file_path}: {e}")
    else:
        log.info("No file to delete or file does not exist.")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='fetch_upload_gcc',
    default_args=default_args,
    description='Fetch news data and upload to GCS',
    schedule='0 1 */2 * *',
    start_date=datetime(2025, 2, 20),
    catchup=False,
    tags=['news', 'gcs'],
) as dag:

    fetch_news_task = PythonOperator(
        task_id='fetch_news_and_save',
        python_callable=fetch_and_save_news,
    )

    upload_task = LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs',
        src="{{ ti.xcom_pull(task_ids='fetch_news_and_save', key='file_path') }}",
        dst="{{ ti.xcom_pull(task_ids='fetch_news_and_save', key='gcs_file_path') }}",
        bucket="msds-697-dag-test",
    )

    delete_file_task = PythonOperator(
        task_id='delete_local_file',
        python_callable=delete_local_file,
    )

    fetch_news_task >> upload_task >> delete_file_task