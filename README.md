# Apache Airflow Data Pipeline

This project demonstrates the creation of an Apache Airflow data pipeline to automate various data-related tasks, including API integration, data extraction, transformation, and interaction with a PostgreSQL database.

## Project Tasks

### Task 1: Check API Availability

**Objective:** Verify the availability of the specified API for making calls.

**Implementation:** This task includes a Python script that checks API availability using relevant modules.

![Http_Connection](/screenshot/HttpConnection.png)

### Task 2: Extract Data from API and Convert to CSV

**Objective:** Extract data from the API and convert it into CSV format, then save it to a local folder.

**Implementation:** This task involves Python scripts utilizing appropriate modules to fetch data from the API and store it as a CSV file in a designated local folder.
![CSV](/screenshot/CSV.png)

### Task 3: CSV Sensor for Folder Monitoring

**Objective:** Set up an Airflow sensor to monitor the "air_flow_project" folder, waiting for the CSV file generated in Task 2 to appear.

**Implementation:** Utilize Airflow's built-in sensor functionality to monitor the folder until the CSV file becomes available.

### Task 4: Load CSV to PostgreSQL

**Objective:** Load the CSV data into a PostgreSQL table using an Airflow operator. Dynamically generate the table name.

**Implementation:** Implement this task using an Airflow operator, ensuring that the CSV data is loaded into a PostgreSQL table with a dynamically generated name.

![postgres](/screenshot/postgres.png)

### Task 5: Run Spark Job for Metrics Calculation

**Objective:** Execute a Spark submit job to calculate simple metrics based on the loaded data. Store results in multiple destinations.

**Implementation:** This task includes a Python script that submits a Spark job for metric calculation. The results are saved in the "Airflow_Output" folder as Parquet files and inserted into a PostgreSQL table named `<table_name>`.

![spark-submit](/screenshot/spark-submit.png)

### Task 6: Read Table from PostgreSQL and Log Results

**Objective:** Read a single table from PostgreSQL and display the results in the "airflow_output.log" file.

**Implementation:** Create a Python script as an Airflow task that retrieves data from the PostgreSQL table and logs it in the specified log file.

![Log-result](/screenshot/result.png)

## Requirements

- Python
- Apache Airflow
- PostgreSQL
- Spark (for Task 5)

## Usage

1. Clone this repository to your local machine:

git clone https://github.com/fuserojeshpradhananga/Airflowiproject

cd airflow-data-pipeline

2. Install the required Python modules using pip:
pip install psycopg2 apache-airflow[postgres]


3. Configure Apache Airflow and set up a DAG to execute the tasks. 

Ensure you provide the necessary API details, file paths, and PostgreSQL connection settings in the DAG configuration.

4. Start the Airflow scheduler and web server:

airflow scheduler
airflow webserver


5. Access the Airflow web interface, trigger the DAG to run, and monitor the progress.

## Customization

- Modify the DAG configuration to fit your specific API, data extraction, and database requirements.
- Customize the Spark job for metrics calculation based on your dataset.

## Contributing

Contributions and improvements are welcome! Feel free to submit pull requests or issues if you encounter any problems or have suggestions for enhancements.

