# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Use this notebook to update the job with the job_cluster details from the table
# MAGIC
# MAGIC Prerequesites -  
# MAGIC 1. Make sure to store job_id and previous job_cluster details in a table for the given job_id

# COMMAND ----------

dbutils.widgets.text("tableToStoreJobs", "srikanth_anumula_catalog. srikanth_uc_migrate.job_config")
tableToStoreJobs = dbutils.widgets.get("tableToStoreJobs")

dbutils.widgets.text("jobId", "some_jobid")
jobId = dbutils.widgets.get("jobId")
print(tableToStoreJobs, jobId)

# COMMAND ----------

import json
import requests as req

# COMMAND ----------

context = json.loads(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
)
instancename = context["tags"]["browserHostName"]
print(instancename)

token = (
    dbutils.widgets._entry_point.getDbutils().notebook().getContext().apiToken().get()
)

api_header = {"Authorization": f"Bearer {token}"}


# COMMAND ----------

# get job cluster details from table and update job cluster config using jobs update API
import ast
jobsDf = spark.sql(f"select cluster_config from {tableToStoreJobs} where job_id = {jobId}")

if jobsDf.isEmpty():
  print(f"job details not available in the table: {tableToStoreJobs} and job id: {jobId}")
else:  
  clusterConfig = jobsDf.collect()[0][0]
  res = ast.literal_eval(clusterConfig)
  data = {}
  new_settings = {}
  data['job_id'] = jobId
  new_settings['job_clusters'] = [res]
  data['new_settings'] = new_settings
  json_data = json.dumps(data, indent=3)
  #print(json_data)

  url = f"https://{instancename}/api/2.1/jobs/update"

  jobUpdateResponse = req.post(url, headers=api_header, data=json_data)

  if jobUpdateResponse.status_code == 200:
    print(f"Updated successfully with old config for job_id: {jobId}")
  else:
    print(f"Error while updating {jobId} : {jobUpdateResponse.content}")
