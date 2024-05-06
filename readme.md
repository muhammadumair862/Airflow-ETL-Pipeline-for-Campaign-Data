**Airflow ETL Pipeline for Generating Noisy Campaign Data**

---

### Overview:
This project automates the generation of noisy campaign data, performs ETL operations on the generated data, and stores it in a MongoDB server. The generated data is then utilized to build a Power BI dashboard for visualization and analysis.

### Project Structure:
- **airflow**: Contains Airflow DAGs (Directed Acyclic Graphs) for orchestrating the data generation and ETL processes.
- **etl**: Contains scripts for performing ETL operations on the generated data.
- **README.md**: Provides an overview of the project, its structure, and instructions for running the pipeline.
- **requirements.txt**: Lists Python dependencies required for the project.

### Dependencies:
Ensure you have Python, Airflow, MongoDB, and Power BI installed on your system. You can install the required Python dependencies using the following command:

```bash
pip install -r requirements.txt
```

### Usage:
1. Clone this repository to your local machine:

```bash
git clone <repository_url>
```

2. Navigate to the project directory:

```bash
cd airflow-campaign-data
```

3. Start Airflow and set up the Airflow scheduler and webserver:

```bash
airflow initdb
airflow webserver -p 8080
airflow scheduler
```

4. Place the MongoDB connection details in the Airflow configuration file.

5. Copy the DAG files from the `airflow/dags` directory to your Airflow DAGs directory.

6. Start the Airflow scheduler and webserver:

```bash
airflow scheduler
airflow webserver -p 8080
```

7. Access the Airflow UI in your web browser (typically at `localhost:8080`) to trigger and monitor the DAGs.

8. Once the data generation and ETL processes are complete, you can use the generated data stored in MongoDB to build Power BI dashboards for visualization and analysis.

### Data Generation:
Two data files are generated:
- One file is generated every hour.
- Another file is generated every 30 minutes with some added noise in the campaign dataset.

### ETL Process:
The ETL script performs transformation operations on the generated data files before storing them in the MongoDB server.

### Building Power BI Dashboard:
Utilize the data stored in the MongoDB server to build a Power BI dashboard for visualization and analysis of campaign data.