from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from huggingface_hub import list_models

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'huggingface_model_etl',
    default_args=default_args,
    description='ETL pipeline for Hugging Face models',
    schedule='@daily',
    catchup=False,
    tags=['etl', 'hugging', 'postgres'],
)

def extract_model_data(**kwargs):
    print("EXTRACT PHASE: Fetching models from Hugging Face Hub...")
    try:
        # Fetching models
        models = list_models(sort="lastModified", direction=-1, limit=50, cardData=True)
        
        raw_models = [{
            "model_id": m.id,
            "author": m.author,
            "pipeline_tag": m.pipeline_tag,
            "tags": m.tags or [],
            "last_modified": m.lastModified.isoformat() if m.lastModified else None,
        } for m in models]
        
   
        kwargs['ti'].xcom_push(key='raw_models', value=raw_models)
        
        print(f"EXTRACT COMPLETE: Retrieved {len(raw_models)} records")
    except Exception as e:
        print(f"Error in EXTRACT phase: {e}")
        raise # Re-raising ensures Airflow marks the task as failed

def transform_model_data(**kwargs):
    ti = kwargs['ti']
    raw_models = ti.xcom_pull(task_ids='extract_huggingface_models', key='raw_models') or []
    
    transformed_data = []
    seen = set()
    
    for m in raw_models:
        mid = m.get('model_id')
        if not mid or mid in seen:
            continue
        seen.add(mid)
        
        # CORRECTED: Variable name consistency
        transformed_data.append({
            "model_id": mid,
            "author": m.get('author') or 'N/A',
            "pipeline_tag": m.get('pipeline_tag') or 'N/A',
            "tags": m.get('tags') or [],
            "last_modified": m.get('last_modified'),
        })
            
    ti.xcom_push(key='transformed_models', value=transformed_data)
    print(f"TRANSFORM COMPLETE: Produced {len(transformed_data)} cleaned records")

def load_to_postgres(**kwargs):
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_model_data', key='transformed_models')
    
    if not transformed_data:
        print("LOAD ERROR: No data found.")
        return

    postgres_hook = PostgresHook(postgres_conn_id='models_connection')
    
    create_table_query = """
    CREATE TABLE IF NOT EXISTS ai_models (
        model_id VARCHAR(255) PRIMARY KEY,
        author VARCHAR(255),
        pipeline_tag VARCHAR(100),
        tags TEXT[],
        last_modified TIMESTAMP
    );
    """
    postgres_hook.run(create_table_query)

    insert_query = """
    INSERT INTO ai_models (model_id, author, pipeline_tag, tags, last_modified)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (model_id) DO UPDATE SET
        author = EXCLUDED.author,
        pipeline_tag = EXCLUDED.pipeline_tag,
        tags = EXCLUDED.tags,
        last_modified = EXCLUDED.last_modified;
    """

    for model in transformed_data:
        postgres_hook.run(insert_query, parameters=(
            model['model_id'],
            model['author'],
            model['pipeline_tag'],
            model['tags'],
            model['last_modified']
        ))


extract_task = PythonOperator(task_id='extract_huggingface_models', python_callable=extract_model_data, dag=dag)
transform_task = PythonOperator(task_id='transform_model_data', python_callable=transform_model_data, dag=dag)
load_task = PythonOperator(task_id='load_to_postgres', python_callable=load_to_postgres, dag=dag)

extract_task >> transform_task >> load_task
