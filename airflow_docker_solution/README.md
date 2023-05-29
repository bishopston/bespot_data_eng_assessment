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

We create a simple DAG (**dag_ingest_process.py**) which runs daily and consists of two tasks which utilize Python Operators. The ingestion task runs first and the process task runs consequently. The logic is the same with previous solutions. The datasets are stored in database after being processed.

By connecting to *localhost:8080*, we verify successful execution in task logs and observe that our data have been inserted in corresponding tables.

Should we want to address the issue of scalability in our pipeline we will have to turn to another kind of Airflow operators, either Celery or Kubernetes operators.
The Celery executor requires to set up Redis or RabbitMQ to distribute messages to workers. Airflow then distributes tasks to Celery workers that can run in one or multiple machines. Therefore this executor enables a certain number of concurrent data engineering tasks. Scaling with the Celery executor involves choosing both the number and size of the workers available to Airflow. The more workers you have available in your environment, or the larger your workers are, the more capacity you have to run tasks concurrently.
The Kubernetes executor launches a pod in a Kubernetes cluster for each task. Since each task runs in its own pod, resources can be specified on an individual task level. It allows you to dynamically scale up and down based on the task requirements. A major difficulty with the Kubernetes operator is that one still needs to understand the Kubernetes configuration system and set up a cluster. It might be easier to enable auto-scaling on their cluster to ensure they get the benefit of Kubernetes' elasticity.


With regards to adding fault tolerance in our pipeline architecture, we could spin up two schedulers running on different machines to provide fault-tolerance. One scheduler is in active and the other one is in standby mode to prevent duplicate job submissions. These two fault-tolerant schedulers are synced via a periodic heartbeat message to see if the remote scheduler is active. Both fault-tolerant schedulers are connected to a common database to simply store relevant information, such as active scheduler, active backup scheduler, and the timestamp of the last heartbeat message. If the active scheduler fails/crashes, the backup scheduler automatically takes over by detecting the absence of a heartbeat message for sometime.