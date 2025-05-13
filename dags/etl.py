from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime  ## Replaces days_ago
import json


## Define the DAG
with DAG(
    dag_id='nasa_apod_postgres',
    start_date=datetime(2024, 5, 9),  ## Set appropriate start date
    schedule='@daily',
    catchup=False

) as dag:
    
    ## step 1: Create the table if it doesn’t exist
    @task
    def create_table():
        ## Initialize the PostgresHook
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")

        ## SQL query to create the table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS apod_data (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
        );
        """
        ## Execute the table creation query
        postgres_hook.run(create_table_query)


    ## Step 2: Extract the NASA API Data (APOD) - Astronomy Picture of the Day [Extract pipeline]
    ## https://api.nasa.gov/planetary/apod?api_key=uE11CVCTkcowPp5qVdRnlcHET6zWtgR2zxmdpoF2


    extract_apod = HttpOperator(
        task_id='extract_apod',
        http_conn_id='nasa_api',  ## Connection ID Defined In Airflow For NASA API
        endpoint='planetary/apod',  ## NASA API endpoint for APOD
        method='GET',
        data={"api_key": "{{ conn.nasa_api.extra_dejson.api_key }}"},  ## Use the API Key from the connection
        headers={"Content-Type": "application/json"},
        log_response=True,
        do_xcom_push=True  ## Ensure response is pushed to XCom
    )

    
    ## Step 3: Transform the data (Pick the information that I need to save)
    @task
    def transform_apod_data(raw_response_text: str):
        response = json.loads(raw_response_text)
        apod_data = {
            'title': response.get('title', ''),
            'explanation': response.get('explanation', ''),
            'url': response.get('url', ''),
            'date': response.get('date', ''),
            'media_type': response.get('media_type', '')
        }
        return apod_data


    ## Step 4: Load the data into Postgres SQL
    @task
    def load_data_to_postgres(apod_data):
        ## Initialize the PostgresHook
        postgres_hook = PostgresHook(postgres_conn_id='my_postgres_connection')

        ## Define the SQL Insert Query
        insert_query = """
        INSERT INTO apod_data (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s);
        """

        ## Execute the SQL Query
        postgres_hook.run(insert_query, parameters=(
            apod_data['title'],
            apod_data['explanation'],
            apod_data['url'],
            apod_data['date'],
            apod_data['media_type']
        ))


    ## Step 5: Define the task dependencies
    create = create_table()
    raw = extract_apod
    transformed = transform_apod_data(raw.output)
    load = load_data_to_postgres(transformed)
    
    create >> raw >> transformed >> load
