# ADM Final Project

**Authors:** Acossi Giorgio, Ventura Eleonora

**Course:** Advanced Data Management - UniGE - 2025/2026

## Overview
This project represents the final implementation step for the Advanced Data Management course. After designing the database structure and defining the workload queries, we developed an ETL pipeline to load football data into a MongoDB cluster. The repository also includes a Jupyter notebook to verify and execute the workload queries.

## Dataset
The dataset used in this project is available on Kaggle:
[**Football Player Scores**](https://www.kaggle.com/datasets/davidcariboo/player-scores?select=appearances.csv)

> Please download the dataset and place the CSV files in the `dataset/` directory before running the pipeline.

## Local Testing with Docker
To set up the environment locally, we use `docker-compose`. This will start the Config Servers, Shards, and Router services, initialize the cluster, and automatically run the ETL process.

### Steps:
1. **Clone the repository**.
2. **Ensure Docker is running**.
3. **Build and start the containers**:
   
   ```bash
   docker-compose up --build
   ```

The `etl` service will automatically:
- Wait for the MongoDB cluster to be ready.
- Clean existing data.
- Setup indexes and sharding.
- Load data from the CSV files in `dataset/`.
- Log progress to the console.

## Environment Configuration
The project uses a `.env` file to manage configuration.

### `.env` Example
Create a file named `.env` in the root directory:

```env
MONGO_URI=mongodb://mongo-router:27017/
DB_NAME=adm_project_db
```

## Connection & Port Forwarding
If you need to connect to the database from your local machine while the database is hosted on a remote server (such as the server provided by UniGE), you can use SSH tunneling or port forwarding.

**SSH Port Forwarding Command:**
To map the remote MongoDB instance to your local port `27018`, run:
```bash
ssh -L 27018:localhost:27017 user@192.168.1.1
```

**Example Connection String:**
Once the tunnel is established:

```text
mongodb://user:password@localhost:27018/adm_project_db?authSource=admin
```

## Running Workload Queries
The queries are defined in `workload.ipynb`.

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure `.env` for Local Execution**:
   If running the notebook locally against the Docker cluster exposed on localhost (default port 27017):
   ```env
   MONGO_URI=mongodb://localhost:27017/
   DB_NAME=adm_project_db
   ```
   
   If using the port forwarding example above:
   ```env
   MONGO_URI=mongodb://user:password@localhost:27018/adm_project_db?authSource=admin
   DB_NAME=adm_project_db
   ```

3. **Run the Notebook**:
   Open `workload.ipynb` in Jupyter or VS Code to execute the queries.
