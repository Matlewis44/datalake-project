# Databricks notebook source
# MAGIC %md
# MAGIC Configure la connexion à Event Hubs.
# MAGIC Définit le schéma JSON des données entrantes.

# COMMAND ----------

from pyspark.sql.types import *
import pyspark.sql.functions as F
from datetime import datetime as dt
import json


connectionString = "Endpoint=sb://hubcontainer.servicebus.windows.net/;SharedAccessKeyName=sharedPolicy;SharedAccessKey=wox9csTZtBxDg4qNV1D4roHfR74GiB4Bp+AEhNhoqys=;EntityPath=streamhandler"
ehConf = {}
ehConf["eventhubs.connectionString"] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)
ehConf["eventhubs.consumerGroup"] = "$Default"

# Define schemas for the different data types
transaction_log_media_schema = StructType([
    StructField("transactionID", IntegerType(), True),
    StructField("customerID", IntegerType(), True),
    StructField("customerEmail", StringType(), True),
    StructField("customerFirstName", StringType(), True),
    StructField("customerLastName", StringType(), True),
    StructField("amount", FloatType(), True),
    StructField("lastTransactionDate", DateType(), True), 
    StructField("type", StringType(), True),

    StructField("logID", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("message", StringType(), True),
    StructField("logDate", DateType(), True),

    StructField("postID", IntegerType(), True),
    StructField("pseudonym", StringType(), True),
    StructField("content", StringType(), True),
    StructField("postDate", DateType(), True),

    StructField("timestamp", TimestampType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC 1. speed Layer

# COMMAND ----------

# MAGIC %md
# MAGIC #### Lecture du Flux de Données depuis Event Hubs
# MAGIC Ce code lit un flux de données en continu depuis Azure Event Hubs et effectue les opérations suivantes :
# MAGIC
# MAGIC 1. **Configuration de la Source** : Utilise les configurations définies dans `ehConf` pour se connecter à Event Hubs.
# MAGIC 2. **Chargement des Données** : Charge les données du flux en continu.
# MAGIC 3. **Conversion JSON** : Convertit le champ `body` en JSON en utilisant le schéma `transaction_log_media_schema`.
# MAGIC 4. **Extraction des Colonnes** : Extrait toutes les colonnes du champ `body`.
# MAGIC
# MAGIC Enfin, le DataFrame résultant est affiché.

# COMMAND ----------

eventhub_stream = (
    spark.readStream.format("eventhubs")
        .options(**ehConf)
        .load()
        .withColumn('body', F.from_json(F.col('body').cast('string'), transaction_log_media_schema))
        .select("body.*")  # extraction de toutes les colonnes
)

display(eventhub_stream)

# COMMAND ----------

# MAGIC %md
# MAGIC 2. batch layer (stockage, raffinement)

# COMMAND ----------

# MAGIC %md
# MAGIC 1. On configure l'accès à un compte de stockage Azure
# MAGIC 2. On définit les chemins pour les différentes couches de données (bronze, silver, gold) ainsi que le chemin des checkpoints. 
# MAGIC 3. Activation optimisation d'écriture et du compactage automatique pour Delta Lake.

# COMMAND ----------

# Setup access to storage account 
storage_account = 'websitedatalake'
spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", 'wL7iZSKLTpuDoBZW3HEyjbhy1pITqa5BdU5x7BFVJuqiU7yzGSKJvsqUeHAUS/NhVhwXO0gtmzjT+ASt0cz8MQ==')


ROOT_PATH = f"abfss://datawebsite@{storage_account}.dfs.core.windows.net/"
BRONZE_PATH = ROOT_PATH + "bronze/"
SILVER_PATH = ROOT_PATH + "silver/"
GOLD_PATH = ROOT_PATH + "gold/"
CHECKPOINT_PATH = ROOT_PATH + "checkpoints/"

# Activation auto compactage et écriture de donneés optimisé pour Delta Lake
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled","true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled","true")
spark.conf.set("spark.databricks.delta.formatCheck.enabled","false")


# COMMAND ----------

# MAGIC %md
# MAGIC Ecriture de la couche bronze dans le datalake

# COMMAND ----------

# MAGIC %md
# MAGIC - On écrit les données en streaming dans des tables Delta brutes
# MAGIC - Tables partitionnées par `lastTransactionDate`.
# MAGIC - Le `checkpointLocation` spécifie l'emplacement où les informations de point de contrôle sont stockées pour permettre la reprise du traitement en cas de panne.

# COMMAND ----------

# Process transactions
transactions_delta_table_raw = (
    eventhub_stream
        .select("customerID", "customerEmail", "customerFirstName", "customerLastName", "transactionID", "amount", "lastTransactionDate", "type","timestamp", )
        .writeStream.format('delta')                                                     
        .partitionBy('lastTransactionDate')                                                          
        .option("checkpointLocation", CHECKPOINT_PATH + "transactions_delta_table_raw")
        #.trigger(once=True)                 
        .start(BRONZE_PATH + "transactions_delta_table_raw")   
)

# Process logs                                    
log_delta_table_raw = (
    eventhub_stream
        .select("customerID", "customerEmail", "logID", "status", "message", "logDate", "timestamp")
        .writeStream.format('delta')                                                     
        .partitionBy('logDate')                                                             
        .option("checkpointLocation", CHECKPOINT_PATH + "log_delta_table_raw")
        #.trigger(once=True)                 
        .start(BRONZE_PATH + "log_delta_table_raw")   
)

# Process social media posts
social_media_table_raw = (
    eventhub_stream
        .select("customerID", "customerEmail", "pseudonym", "postID", "content", "postDate", "type","timestamp")
        .writeStream.format('delta')                                                     
        .partitionBy('postDate')                                                             
        .option("checkpointLocation", CHECKPOINT_PATH + "social_media_table_raw")
        #.trigger(once=True)                 
        .start(BRONZE_PATH + "social_media_table_raw")   
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pourquoi créer des tables externes ?
# MAGIC Les tables externes (ou non gérées) permettent de séparer la gestion des fichiers de données de leur enregistrement dans le metastore. Cela signifie que les données peuvent être gérées indépendamment du cycle de vie des tables dans le metastore. Les tables externes sont utiles pour :
# MAGIC
# MAGIC - Référencer des données existantes sans les déplacer.
# MAGIC - Gérer les données de manière plus flexible, par exemple en les partageant entre plusieurs clusters ou en les utilisant avec d'autres outils.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS datalake_project.bronze.transactions_delta_table_raw USING DELTA LOCATION '{BRONZE_PATH}transactions_delta_table_raw'
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS datalake_project.bronze.log_delta_table_raw USING DELTA LOCATION '{BRONZE_PATH}log_delta_table_raw'
""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS datalake_project.bronze.social_media_table_raw USING DELTA LOCATION '{BRONZE_PATH}social_media_table_raw'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Ces commandes SQL enregistrent les tables dans le metastore de Databricks, permettant ainsi de les interroger et de les gérer via SQL tout en conservant les données à leur emplacement d'origine.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from datalake_project.bronze.social_media_table_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Filter by partition column to improve query performance
# MAGIC select * 
# MAGIC from datalake_project.bronze.transactions_delta_table_raw
# MAGIC where lastTransactionDate between '2023-11-14' and '2024-01-14';

# COMMAND ----------

# MAGIC %md
# MAGIC https://docs.databricks.com/en/structured-streaming/delta-lake.html#language-python

# COMMAND ----------

# MAGIC %md
# MAGIC - La fonction `merge_delta` fusionne les données d'une DataFrame `incremental` dans une table Delta `target`.
# MAGIC - On crée une vue temporaire "incremental" et on tente de fusionner les enregistrements sur la clé `transactionID`. 
# MAGIC - Si une correspondance est trouvée (WHEN MATCHED), les enregistrements sont mis à jour. Sinon, les enregistrements sont insérés.
# MAGIC - Si la table cible n'existe pas, elle est créée et les données sont écrites.

# COMMAND ----------

# Fonction pour fusionner les données dans la table Delta
def merge_delta(incremental, target): 
    incremental.createOrReplaceTempView("incremental")
    
    try:
        # Merge records into the target table using the specified join key
        incremental._jdf.sparkSession().sql(f"""
            MERGE INTO delta.`{target}` t
            USING incremental i
            ON  i.transactionID = t.transactionID
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    except:
        incremental.write.format("delta").mode("overwrite").save(target)

# COMMAND ----------

# # Silver Layer: Aggregate Transactions
# silver_transactions_delta_table = (spark.readStream.format("delta").table("datalake_project.bronze.transactions_delta_table_raw").
#         groupBy("customerID", "lastTransactionDate", F.window("timestamp","30 minutes"))
#         .agg(F.sum("amount").alias("total_amount"), F.count("transactionID").alias("transaction_count"))
#         .writeStream
#         .foreachBatch(lambda i,b: merge_delta(i,SILVER_PATH+"silver_transactions_delta_table"))
#         .outputMode("update")
#         .option('checkpointLocation',CHECKPOINT_PATH+'silver_transactions_delta_table')
#         # .trigger(once=True)
#         .start()
# )

# # Silver Layer: Aggregate Logs
# silver_logs_delta_table = (spark.readStream.format("delta").table("datalake_project.bronze.log_delta_table_raw").
#         groupBy("customerID", "logDate", F.window("timestamp","30 minutes"))
#         .agg(F.count("logID").alias("log_count"))
#         .writeStream
#         .foreachBatch(lambda i,b: merge_delta(i,SILVER_PATH+"silver_logs_delta_table"))
#         .outputMode("update")
#         .option('checkpointLocation',CHECKPOINT_PATH+'silver_logs_delta_table')
#         # .trigger(once=True)
#         .start()
# )

# # Silver Layer: Aggregate Social Media Posts
# silver_social_media_table = (spark.readStream.format("delta").table("datalake_project.bronze.social_media_table_raw").
#         groupBy("customerID", F.window("timestamp","30 minutes"))
#         .agg(F.count("postID").alias("post_count"))
#         .writeStream
#         .foreachBatch(lambda i,b: merge_delta(i,SILVER_PATH+"silver_social_media_table"))
#         .outputMode("update")
#         .option('checkpointLocation',CHECKPOINT_PATH+'silver_social_media_table')
#         # .trigger(once=True)
#         .start()
# )

# COMMAND ----------

# MAGIC %md
# MAGIC - Aggrégation des transactions, journaux et publications sur les réseaux sociaux à partir de tables Delta brutes (bronze)
# MAGIC - Regroupement par client et par fenêtre de temps de 30 minutes. 
# MAGIC - Résultats écrits dans des tables Delta de transformation (silver) avec des mises à jour en continu.

# COMMAND ----------

# Silver Layer: Aggregate Transactions
silver_transactions_delta_table = (
    spark.readStream.format("delta").table("datalake_project.bronze.transactions_delta_table_raw")
        .groupBy("customerID", "customerEmail", "customerFirstName", "customerLastName", "lastTransactionDate", F.window("timestamp", "30 minutes"))
        .agg(F.sum("amount").alias("total_amount"), F.count("transactionID").alias("transaction_count"))
        .writeStream
        .foreachBatch(lambda i, b: merge_delta(i, SILVER_PATH + "silver_transactions_delta_table"))
        .outputMode("update")
        .option('checkpointLocation', CHECKPOINT_PATH + 'silver_transactions_delta_table')
        .start()
)

# Silver Layer: Aggregate Logs
silver_logs_delta_table = (
    spark.readStream.format("delta").table("datalake_project.bronze.log_delta_table_raw")
        .groupBy("customerID", "customerEmail", "logDate", F.window("timestamp", "30 minutes"))
        .agg(F.count("logID").alias("log_count"))
        .writeStream
        .foreachBatch(lambda i, b: merge_delta(i, SILVER_PATH + "silver_logs_delta_table"))
        .outputMode("update")
        .option('checkpointLocation', CHECKPOINT_PATH + 'silver_logs_delta_table')
        .start()
)

# Silver Layer: Aggregate Social Media Posts
silver_social_media_table = (
    spark.readStream.format("delta").table("datalake_project.bronze.social_media_table_raw")
        .groupBy("customerID", "customerEmail", F.window("timestamp", "30 minutes"))
        .agg(F.count("postID").alias("post_count"))
        .writeStream
        .foreachBatch(lambda i, b: merge_delta(i, SILVER_PATH + "silver_social_media_table"))
        .outputMode("update")
        .option('checkpointLocation', CHECKPOINT_PATH + 'silver_social_media_table')
        .start()
)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "abfss://datawebsite@websitedatalake.dfs.core.windows.net/silver"

# COMMAND ----------

# MAGIC %md
# MAGIC #####  Creation tables Delta de la couche silver dans le metastore Unity Catalog.
# MAGIC
# MAGIC #####  métadonnées copié depuis le data lake ABFSS où se situe les données
# MAGIC

# COMMAND ----------

# Créer la table Silver pour les transactions
spark.sql(f"""
CREATE TABLE IF NOT EXISTS datalake_project.silver.silver_transactions_delta_table 
USING DELTA LOCATION "abfss://datawebsite@websitedatalake.dfs.core.windows.net/silver/silver_transactions_delta_table/"
""")

# Créer la table Silver pour les journaux
spark.sql(f"""
CREATE TABLE IF NOT EXISTS datalake_project.silver.silver_logs_delta_table 
USING DELTA LOCATION "{SILVER_PATH}silver_logs_delta_table"
""")

# Créer la table Silver pour les publications sur les réseaux sociaux
spark.sql(f"""
CREATE TABLE IF NOT EXISTS datalake_project.silver.silver_social_media_table 
USING DELTA 
LOCATION "{SILVER_PATH}silver_social_media_table"
""")

# COMMAND ----------

# MAGIC %md
# MAGIC - Le gold_table combine les info des transactions, des logs et des médias sociaux. 
# MAGIC - permet d'avoir une vue enrichie par plusieurs sources pour des analyses plus approfondie

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from datalake_project.silver.silver_transactions_delta_table;

# COMMAND ----------

# MAGIC %md
# MAGIC 3. serving Layer
# MAGIC * préparation de la vue enrichie par las tables de la couche silver
# MAGIC - On lit les tables silver Delta de transactions, logs et médias sociaux, 
# MAGIC - Jointure customerID et window pour créer une vue enrichie avec des informations combinées.

# COMMAND ----------

from pyspark.sql.functions import col

# Enriched Table
silver_table_agg = spark.readStream.format('delta').option('ignoreChanges', True).table('datalake_project.silver.silver_transactions_delta_table')
silver_table_agg2 = spark.readStream.format('delta').option('ignoreChanges', True).table('datalake_project.silver.silver_logs_delta_table')
silver_table_agg3 = spark.readStream.format('delta').option('ignoreChanges', True).table('datalake_project.silver.silver_social_media_table')

table_enriched = silver_table_agg.alias("a").join(
    silver_table_agg2.alias("b"), 
    (col("a.customerID") == col("b.customerID")) & (col("a.window") == col("b.window"))
).join(
    silver_table_agg3.alias("c"), 
    (col("a.customerID") == col("c.customerID")) & (col("a.window") == col("c.window"))
).select(
    "a.customerID", "a.customerEmail", "a.customerFirstName", "a.customerLastName", "a.lastTransactionDate", "a.total_amount", "a.transaction_count", "b.log_count", "c.post_count"
)

# COMMAND ----------

# MAGIC %md
# MAGIC - Le gold_table combine les info des transactions, des logs et des médias sociaux.
# MAGIC - permet d'avoir une vue enrichie par plusieurs sources pour des analyses plus approfondie

# COMMAND ----------

gold_table = (
  table_enriched
    .selectExpr('customerID', "customerEmail", "customerFirstName", "customerLastName",'lastTransactionDate','total_amount','transaction_count','log_count','post_count')
    .writeStream 
    .foreachBatch(lambda i, b: merge_delta(i, GOLD_PATH + "gold_table"))
    .option("checkpointLocation", CHECKPOINT_PATH + "gold_table")
    .outputMode("append")
    #.trigger(once=True)
    .start()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Création table Gold si elle n'existe pas déjà
# MAGIC CREATE TABLE IF NOT EXISTS datalake_project.gold.gold_table
# MAGIC USING DELTA
# MAGIC LOCATION '{GOLD_PATH}gold_table'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from datalake_project.gold.gold_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from datalake_project.gold.gold_table where customerID = 29 ;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customerID, 
# MAGIC        MAX(customerEmail) AS customerEmail, 
# MAGIC        MAX(customerFirstName) AS customerFirstName, 
# MAGIC        MAX(customerLastName) AS customerLastName, 
# MAGIC        MAX(lastTransactionDate) AS lastTransactionDate, 
# MAGIC        SUM(total_amount) AS total_amount, 
# MAGIC        SUM(transaction_count) AS transaction_count, 
# MAGIC        SUM(log_count) AS log_count, 
# MAGIC        SUM(post_count) AS post_count
# MAGIC FROM datalake_project.gold.gold_table 
# MAGIC GROUP BY customerID;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customerID, 
# MAGIC        ANY_VALUE(customerEmail) AS customerEmail, 
# MAGIC        ANY_VALUE(customerFirstName) AS customerFirstName, 
# MAGIC        ANY_VALUE(customerLastName) AS customerLastName, 
# MAGIC        ANY_VALUE(lastTransactionDate) AS lastTransactionDate, 
# MAGIC        ANY_VALUE(total_amount) AS total_amount, 
# MAGIC        ANY_VALUE(transaction_count) AS transaction_count, 
# MAGIC        ANY_VALUE(log_count) AS log_count, 
# MAGIC        ANY_VALUE(post_count) AS post_count
# MAGIC FROM datalake_project.gold.gold_table 
# MAGIC GROUP BY customerID;

# COMMAND ----------

# MAGIC %md
# MAGIC Les code ci-dessous ne sont pas à,prende en compte car le travail d'api est réalisé d'une autre comme expliquée dans le rapport. Nénamoins cela représente :
# MAGIC - une utilisation du micro-framework léger et flexible Flask pour création d'api simple
# MAGIC - redémarrage le noyau Python pour utiliser les packages mis à jour
# MAGIC - appel d'api en local

# COMMAND ----------

!pip install flask

# COMMAND ----------

from flask import Flask, jsonify
from pyspark.sql.functions import col
import threading

app = Flask(__name__)

@app.route('/gold_table', methods=['GET'])
def get_gold_table():
    gold_table_df = spark.read.format('delta').table('datalake_project.gold.gold_table')
    gold_table_data = gold_table_df.select(
        col('customerID'),
        col('customerEmail'),
        col('customerFirstName'),
        col('customerLastName'),
        col('lastTransactionDate'),
        col('total_amount'),
        col('transaction_count'),
        col('log_count'),
        col('post_count')
    ).collect()
    
    result = [row.asDict() for row in gold_table_data]
    return jsonify(result)

def run_app():
    app.run(host='0.0.0.0', port=3000)

# On lance le serveur Flask dans un thread séparé
thread = threading.Thread(target=run_app)
thread.start()

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import requests

response = requests.get('http://localhost:5000/gold_table')
print(response.json())
