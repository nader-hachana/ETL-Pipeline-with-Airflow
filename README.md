# Download Airflow
>sudo pip install "apache-airflow==2.3.1" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.3.1/constraints-3.9.txt"

# Run Airflow server
>airflow standalone

# Configure airflow and make it points to the current directory
>nano ~/airflow/airflow.cfg
dags_folder = /home/nader/airflow/dags ==> dags_folder = /mnt/c/Users/Nader Hachana/OneDrive/Documents/Projects/ETL_Pipeline_Airflow

# Download the data
>wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz

# Finally Run the dag on the airflow UI or through CLI
>airflow dags trigger  [-v or --verbose] ETL_Pipeline

# Visualize the result on the terminal
>airflow dags show podcast_summary

# Save the Dag graph as an image on the local directory
>sudo apt-get install graphviz
>airflow dags show podcast_summary --save podcast_summary.png