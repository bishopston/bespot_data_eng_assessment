### Airflow docker solution

For the optional assessment item of automating the pipeline execution we choose Apache Airflow platform running in Docker containers.
Firstly, we fetch the **docker-compose.yaml**

```
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.6.1/docker-compose.yaml'
```

We additionally insert to docker-compose the services we used in docker solution (spark nodes and postgres).

For the purposes of creating a simple DAG we will use a Local Executor. Therefore we config in docker-compose

```
AIRFLOW__CORE__EXECUTOR: LocalExecutor
```

We'll also delete the example DAGs. Therefore we config in docker-compose

```
AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
```

Finally, within docker-compose we delete the Celery related lines, Celery worker and flower, Redis dependency and definition of Redis service. We create directories for dags, logs, plugins, config and set the environmental variable for AIRFLOW_UID.

```
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
```
We mount volumes within docker-compose for the directories of datasets (input csv files) and jars (for Postgres connectivity).

We run database migrations and create the first user account

```
docker compose up airflow-init
```

Here we use a Dockerfile to build a custom image which will contain apache-spark airflow-providers and java. Therefore in docker-compose we comment out the image parameter and uncomment the *build .*

```
#image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.6.1}
build: .
```

We finally create the containers by

```
docker-compose up -d --build
```

We can enter Postgres shell

```
sudo docker exec -it <postgres_container_name> /bin/bash
psql -p 5432 -d airflow -U airflow -h localhost
```

and create *bespot* database and necessary tables.

We create a simple DAG which runs daily and consists of two tasks which utilize Python Operators. The ingestion task runs first and the process task runs consequently. The logic is the same with previous solutions. The datasets are stored in database after being processed.

By connecting to *localhost:8080*, we verify successful execution in task logs and observe that our data have been inserted in corresponding tables.