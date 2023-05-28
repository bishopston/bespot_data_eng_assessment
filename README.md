### Local installation solution

Assuming we have Apache Spark and PostgreSQL locally installed in our system, this solution utilizes Jupyter Notebook to handle the ETL process for our data. PySpark and Findspark need also be installed in our local environment.

```
pip install pyspark
pip install findspark
```
For the data ingestion process **(ingest_pages_trans.ipynb)**, first we create the necessary tables in our database (see DDL script). Then we setup a Spark session including the jar file in our config for connection to Postgres. Through Spark API we read the input csv datasets using the appropriate schemas (i.e. we are not infering the schema). Current timestamp is added to the datasets to be included in the corresponding database table. The transformed datasets are finally written in the corresponding tables. Ingestion data are inserted in "append" mode as they are added to the already existing data in the process tables.

For the transformation process **(process_pages_trans.ipynb)** we read the ingested data from the database and assign them to Spark datasets. Then we proceed with transformations according to the four assessment questions and by making the following assumptions:

* *Which are the most popular pages of the e-shop?*
  We assume popularity of each page is counted by number of occurrences in "dataset1.csv" independent of user/session_id (i.e. a page hit two times by a user within same session counts as two occurrences). 
* *How many users visit these pages?*
  We assume a user visiting a page more than once (e.g. twice), within the same session, counts as one user occurrence (e.g. not two).
* *How many transactions are performed on each page?*
  We assume that for each session a transaction happens at the page whose timestamp is closer to the timestamp of the transaction that happens within this particular session.
* *What is the average time to purchase for a user?*
  We assume that time to purchase for a user within a specific session is considered the time between the earliest timestamp within this session and the transaction timestamp of the same session, independent of the kind of transaction (i.e. whether it is “A” or “B”). 

For the 3rd and 4th questions we need first to join the two datasets on two fields (user and session_id). After creating the target datasets for each question, we write them in the corresponding database tables with "overwrite" mode as we need to have the new view on the four questions.