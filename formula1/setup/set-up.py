# Databricks notebook source
# get secrets from azure key vault
client_id = dbutils.secrets.get(scope='formula1-socpe',key='client-key')
tenant_id = dbutils.secrets.get(scope='formula1-socpe',key='tenant-key')
client_secret = dbutils.secrets.get(scope='formula1-socpe',key='client-secret')


# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

data_lake_name = "deformula1dl"

def mount_container(container_name):
    dbutils.fs.mount(
    source = f"abfss://{container_name}@deformula1dl.dfs.core.windows.net/",
    mount_point = f"/mnt/{data_lake_name}/{container_name}",
    extra_configs = configs)

mount_container('raw')
mount_container('processed')
mount_container('presentation')


# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# data_lake_name = "deformula1dl"
# for container in ["raw","processed","presentation"]:
#     dbutils.fs.unmount(f'/mnt/{data_lake_name}/{container}')

# COMMAND ----------


