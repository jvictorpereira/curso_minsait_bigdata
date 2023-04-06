
# coding: utf-8

# In[67]:


from pyspark.sql import SparkSession, dataframe, types
from pyspark.sql.functions import when, col, sum, count, isnan, round, trim, split
from pyspark.sql.functions import regexp_replace, concat_ws, sha2, rtrim, substring
from pyspark.sql.functions import unix_timestamp, from_unixtime, to_date
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql.functions import isnull, count
from pyspark.sql import HiveContext
from pyspark.sql.functions import to_date, date_format, year, month, dayofmonth, quarter

import os
import re
import pandas as pd


from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import when

spark = SparkSession.builder.master("local[*]")    .enableHiveSupport()    .getOrCreate()


# In[68]:


def salvar_df(df, file):
    output = "/input/desafio_curso/gold/" + file
    erase = "hdfs dfs -rm " + output + "/*"
    rename = "hdfs dfs -get /datalake/gold/"+file+"/part-* /input/desafio_curso/gold/"+file+".csv"
    print(rename)
    
    
    df.coalesce(1).write        .format("csv")        .option("header", True)        .option("delimiter", ";")        .mode("overwrite")        .save("/datalake/gold/"+file+"/")

    os.system(erase)
    os.system(rename)


# In[76]:


# Criação dos dataframes diretamente do Hive

df_vendas = spark.sql("select * from desafio_curso.vendas")
df_clientes = spark.sql("select * from desafio_curso.clientes")
df_endereco = spark.sql("select * from desafio_curso.endereco")
df_regiao = spark.sql("select * from desafio_curso.regiao")
df_divisao = spark.sql("select * from desafio_curso.divisao")


# In[77]:


# df resultado do join entre df vendas e df clientes
df = df_vendas.join(df_clientes, df_vendas.customer_key == df_clientes.customer_key, "inner").drop(df_clientes.customer_key)


# In[78]:


# iniciando os join para o stage final
df_stage = df.join(df_endereco, df_clientes.address_number == df_endereco.address_number, "left").drop(df_clientes.address_number)


# In[79]:


df_stage = df_stage.join(df_regiao, df_clientes.region_code == df_regiao.region_code, "left").drop(df_clientes.region_code)


# In[80]:


#stage final resultado de todos os join
df_stage_final = df_stage.join(df_divisao, df_clientes.division == df_divisao.division, "left").drop(df_clientes.division)


# In[84]:


# Regra de validação para preencher campos em brancos ou vazios com "Não informado"

for column_name in df_stage_final.columns:
    df_stage_final = df_stage_final.withColumn(column_name, trim(when(col(column_name).isNull() | (col(column_name) == ''), "Não informado").otherwise(col(column_name))))


# In[88]:


#Criando as colunas dia, mês, ano e trimestre para a dimensão tempo
df_stage_final = df_stage_final.withColumn("data", to_date("invoice_date", "dd/MM/yyyy"))
df_stage_final = df_stage_final.withColumn("dia", dayofmonth("data"))
df_stage_final = df_stage_final.withColumn("mes", month("data"))
df_stage_final = df_stage_final.withColumn("ano", year("data"))
df_stage_final = df_stage_final.withColumn("trimestre", quarter("data")).drop(df_stage_final.invoice_date)                   


# In[90]:


#Criação das chaves
df_stage_final = df_stage_final.withColumn('PK_LOCALIDADE', sha2(concat_ws("", df_stage_final.customer_address_1, df_stage_final.customer_address_2, df_stage_final.customer_address_3, df_stage_final.customer_address_4, df_stage_final.address_number, df_stage_final.region_code, df_stage_final.region_name, df_stage_final.city, df_stage_final.country, df_stage_final.state, df_stage_final.zip_code ), 256)) 


# In[100]:


df_stage_final = df_stage_final.withColumn('PK_TEMPO', sha2(concat_ws("", df_stage_final.data, df_stage_final.dia, df_stage_final.mes, df_stage_final.ano, df_stage_final.trimestre), 256))


# In[93]:


df_stage_final = df_stage_final.withColumn('PK_CLIENTES', sha2(concat_ws("", df_stage_final.customer_key, df_stage_final.customer, df_stage_final.customer_type, df_stage_final.phone, df_stage_final.line_of_business), 256))


# In[107]:


#Criando tabela temporaria para criar a fatos e as dimensões
df_stage_final.createOrReplaceTempView("stage")


# In[101]:


ft_vendas = spark.sql("SELECT PK_CLIENTES, PK_TEMPO, PK_LOCALIDADE, COUNT(sales_price) AS VALOR_DE_VENDA from stage group by PK_CLIENTES, PK_TEMPO, PK_LOCALIDADE")


# In[38]:


df_clientes = spark.sql("SELECT DISTINCT PK_CLIENTES, customer_key, customer, customer_type, phone, line_of_business FROM STAGE")


# In[39]:


df_localidade = spark.sql("SELECT DISTINCT PK_LOCALIDADE, customer_address_1, customer_address_2, customer_address_3, customer_address_4, address_number, region_code, region_name, city, country, state, zip_code FROM STAGE")


# In[97]:


df_tempo = spark.sql("SELECT DISTINCT PK_TEMPO, data, dia, mes, ano, trimestre FROM STAGE")


# In[103]:


#Salvar os resultados na pasta Gold
salvar_df(ft_vendas, 'ft_vendas')


# In[43]:


salvar_df(df_clientes, 'dim_clientes')


# In[44]:


salvar_df(df_localidade, 'dim_localidade')


# In[99]:


salvar_df(df_tempo, 'dim_tempo')

