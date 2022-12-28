# import the libraries

from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

#defining DAG arguments
# WHAT THE FK
default_args = {
    'owner': 'Nader Hachana',
    'start_date': days_ago(0),
    'email': ['naderhachana96@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# define the DAG
dag = DAG(
    dag_id='ETL_Pipeline',
    default_args=default_args,
    description='ETL_ Pipeline',
    schedule_interval=timedelta(days=1),
)

# define the tasks

unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xvzf tolldata.tgz -C ./data/',
    dag=dag,
    cwd=dag.folder 
# or:cwd= '/mnt/c/Users/Nader Hachana/OneDrive/Documents/Projects/ETL_Pipeline_Airflow'
# this way its easier without needing to change the dags_folder in airflow.cfg
)

extract_data_from_csv= BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d"," -f1-4 ./data/vehicle-data.csv > ./data/extracted_data/csv_data.csv',
    dag=dag,
    cwd=dag.folder # the cwd option is to set the working directory you would like for the command to be executed in. 
                   # If the option isn't passed, the command is executed in a temporary directory.
                   # It was added in Airflow 2.2
)

extract_data_from_tsv= BashOperator(
    task_id='extract_data_from_tsv',
    bash_command= 'sed "s/\t/,/g" < ./data/tollplaza-data.tsv | cut -d"," -f5-7 | tr -d "\r" > ./data/extracted_data/tsv_data.csv',  # tr -d "\r" deletes line endings
            # or: 'tr "\t" "," < ./data/tollplaza-data.tsv | cut -d"," -f5-7 | tr -d "\r" > ./data/extracted_data/tsv_data.csv'
    dag=dag,
    cwd=dag.folder
)

extract_data_from_fixed_width= BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='sed "s/ \+/,/g" < ./data/payment-data.txt | cut -d"," -f11,12 > ./data/extracted_data/fixed_width_data.csv',
            # or: 'tr -s " " < ./data/payment-data.txt | tr " " "," | cut -d"," -f11,12 > ./data/extracted_data/fixed_width_data.csv'
    dag=dag,
    cwd=dag.folder
)

consolidate_data= BashOperator(
    task_id='consolidate_data',
    bash_command=' paste -d"," ./data/extracted_data/csv_data.csv ./data/extracted_data/tsv_data.csv ./data/extracted_data/fixed_width_data.csv > ./data/extracted_data/extracted_data.csv',
    dag=dag,
    cwd=dag.folder
)

transform_data= BashOperator(
    task_id='transform_data',
    bash_command='awk -F\, \'{$4=toupper($4)}1\' OFS=\, ./data/extracted_data/extracted_data.csv > ./data/extracted_data/transformed_data.csv',
            # or: 'awk -i inplace \'BEGIN{FS=","; OFS=","} {$4=toupper($4)} 1\' ./data/extracted_data/extracted_data.csv'
    dag=dag,
    cwd=dag.folder
)

# task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
