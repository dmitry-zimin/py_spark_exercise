# Crude Oil Analysis dockerized stand-alone single node Spark cluster

## Description
This project sets up a Dockerized single-node Spark cluster and runs Spark jobs to analyze a Crude Oil dataset from Kaggle.

## Prerequisites
- Docker

## Setup and Usage

1. **Clone the repository:**
   ```sh
   git clone https://github.com/dmitry-zimin/py_spark_exercise.git
   cd py_spark_exercise

2. **Download the Dataset:**
   
   Download the Crude Oil dataset from Kaggle and place it in the data directory. Ensure the file is named data.csv
   https://www.kaggle.com/datasets/alistairking/u-s-crude-oil-imports/data
   

3. **Build the Docker Image:**

`docker build -t spark-single-node .
`

4. **Run the Docker Container with Mounted Volumes:**

`docker run -d --name spark-single-node -p 8080:8080 -p 7077:7077 -v $(pwd):/opt/spark/project spark-single-node
`

5. **Submit the Spark Jobs:**

`docker exec -it spark-single-node /opt/spark/bin/spark-submit /opt/spark/project/spark_jobs.py
`



