-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Créer les catalogues et schémas nécessaires pour ce projet
-- MAGIC 1. Catalogue - datalake_project (sans emplacement géré)
-- MAGIC 2. Schémas - bronze, silver & gold (avec emplacements gérés)

-- COMMAND ----------

-- Requêter une table dont on ne connais plus le schéma
SELECT * FROM system.information_schema.tables
WHERE table_name = 'transactions_delta_table_raw';

-- COMMAND ----------

CREATE catalog if NOT EXISTS datalake_project;

-- COMMAND ----------

Use CATALOG datalake_project;  

-- COMMAND ----------

CREATE SCHEMA if NOT EXISTS bronze
MANAGED LOCATION 'abfss://datawebsite@websitedatalake.dfs.core.windows.net/bronze'

-- COMMAND ----------

CREATE SCHEMA if NOT EXISTS silver
MANAGED LOCATION 'abfss://datawebsite@websitedatalake.dfs.core.windows.net/silver'

-- COMMAND ----------

CREATE SCHEMA if NOT EXISTS gold
MANAGED LOCATION 'abfss://datawebsite@websitedatalake.dfs.core.windows.net/gold'

-- COMMAND ----------

SHOW SCHEMAS;
