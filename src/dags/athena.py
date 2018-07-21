import boto3, botocore, sys
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


# SPECIFIC DAG SETTINGS
START_DATE = datetime(2010, 1, 1)
END_DATE = datetime(2010, 3, 31)
BUCKET_NAME = 'datacenternotes-athena'
ATHENA_DB = 'historydb'
ATHENA_TABLE = 'events_table'
AWS_REGION = 'eu-central-1'
AWS_ACCESS_KEY_ID = ''  # Add your own credentials here
AWS_SECRET_ACCESS_KEY = ''  # Add your own credentials here


def check_if_file_for_date(ds: str, **kwargs) -> None:
    date_year = ds[:4]
    date_month = ds[5:7]
    date_day = ds[8:]
    session = boto3.Session(region_name=kwargs['AWS_REGION'],
                            aws_access_key_id=kwargs['AWS_ACCESS_KEY_ID'],
                            aws_secret_access_key=kwargs['AWS_SECRET_ACCESS_KEY'])
    s3client = session.client('s3')

    try:
        print('Found files for {}'.format(ds))
        a = s3client.get_object(Bucket=BUCKET_NAME, Key='data/year={}/month={}/day={}/data.csv'.format(
            date_year, date_month, date_day))
        add_partition = True
    except Exception:
        add_partition = False
        print("There are NO world events for {}".format(ds))

    ti = kwargs['ti']
    ti.xcom_push(key='add_partition', value=add_partition)
    return add_partition


def execute_query(ds: str, **kwargs) -> None:
    ti = kwargs['ti']
    add_partition = ti.xcom_pull(task_ids='check_for_new_files',
                                 key='add_partition')
    if add_partition == False:
        pass
    else:
        print("Adding Partition")
        session = boto3.Session(region_name=kwargs['AWS_REGION'],
                                aws_access_key_id=kwargs['AWS_ACCESS_KEY_ID'],
                                aws_secret_access_key=kwargs['AWS_SECRET_ACCESS_KEY'])
        client = session.client(service_name='athena')
        result_configuration = {"OutputLocation": "s3://{}/".format(BUCKET_NAME)}
        date_year = ds[:4]
        date_month = ds[5:7]
        date_day = ds[8:]
        query = """
                ALTER TABLE {database}.{table}
                ADD PARTITION ( year='{year}', month='{month}', day='{day}' )
                location 's3://{bucket}/data/year={year}/month={month}/day={day}/';
                """.format(database=ATHENA_DB, table=ATHENA_TABLE, year=date_year,
                           month=date_month, day=date_day, bucket=BUCKET_NAME)
        query_response = client.start_query_execution(
            QueryString=query,
            ResultConfiguration=result_configuration
        )
        return query_response


default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2010, 1, 1),
    'end_date': datetime(2010, 3, 31),
    'retries': 1,
    'retry_delay': timedelta(hours=1),
}


dag = DAG(dag_id='add-events-partition-to-athena',
          default_args=default_args,
          schedule_interval='@daily',
          default_view='graph',
          max_active_runs=1)

task_1 = PythonOperator(task_id='check_for_new_files',
                        default_args=default_args,
                        python_callable=check_if_file_for_date,
                        dag=dag,
                        op_kwargs={'AWS_REGION': AWS_REGION,
                                   'AWS_ACCESS_KEY_ID': AWS_ACCESS_KEY_ID,
                                   'AWS_SECRET_ACCESS_KEY': AWS_SECRET_ACCESS_KEY},
                        provide_context=True)

task_2 = PythonOperator(task_id='update_partition',
                        default_args=default_args,
                        python_callable=execute_query,
                        dag=dag,
                        op_kwargs={'AWS_REGION': AWS_REGION,
                                   'AWS_ACCESS_KEY_ID': AWS_ACCESS_KEY_ID,
                                   'AWS_SECRET_ACCESS_KEY': AWS_SECRET_ACCESS_KEY},
                        provide_context=True)

task_1 >> task_2
