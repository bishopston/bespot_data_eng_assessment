### Docker solution

For docker solution we utilize the reliable and widely adopted Bitnami Spark Docker Image and a Postgres image.
First we get the Bitnami **docker-compose.yml** 

```
curl -LO https://raw.githubusercontent.com/bitnami/containers/main/bitnami/spark/docker-compose.yml
```

and we configure a cluster consisting of a master node and a worker node. We configure the mounted volumes for both services (spark and postgres) so that we have persistent database data and we can access our datasets from within spark containers. Then we spin up our three containers (2 spark, 1 postgres) and we are ready to run our pipeline.

```
docker-compose up -d
```

We can have access to spark's master bash.

```
docker exec -it <spark_master_container_name> /bin/bash
```

We can enter into postgres shell and create the necessary tables at the *bespot* database.

```
docker exec -it <postgres_container_name> /bin/bash
psql -p 5432 -d bespot -U admin -h localhost
```

The ingestion and transformation process are handled by two python files (**bespot_ingest.py** and **bespot_process.py** correspondingly) in the same manner as in local deployment solution.

We can run the scripts through spark master shell.

```
python bespot_ingest.py
python bespot_process.py
```