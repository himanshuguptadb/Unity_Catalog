# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Use this notebook to get list of jobs in a workspace and upgrade to UC
# MAGIC
# MAGIC Prerequesites -  
# MAGIC 1. Make sure to SYNC all Databases and Tables to UC 
# MAGIC 1. Ensure external metasore objects are accessible
# MAGIC 1. Create a table to store old job cluster configs in case of failures to go back to previous cluster setup

# COMMAND ----------

import json
import requests as req

# COMMAND ----------

#This code removes any keys(including nested) from the given dictionary

def remove_nested_keys(dictionary, keys_to_remove):
    for key in keys_to_remove:
        if key in dictionary:
            del dictionary[key]

    for value in dictionary.values():
        if isinstance(value, dict):
            remove_nested_keys(value, keys_to_remove)

    return dictionary

# COMMAND ----------

#Initializing workspaceName and token
context = json.loads(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
)
instancename = context["tags"]["browserHostName"]
print(f'instancename: {instancename}')

token = (
    dbutils.widgets._entry_point.getDbutils().notebook().getContext().apiToken().get()
)

api_header = {"Authorization": f"Bearer {token}"}


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Below code takes job_id as parameter and upgrade job to UC
# MAGIC
# MAGIC 1. updates spark_version to  **12.2.x-scala2.12**.   
# MAGIC 1. updates spark_conf to **"spark.databricks.sql.initial.catalog.name":"ucdev"** to use UC catalog
# MAGIC 1. updates data_security_mode to **SINGLE_USER**
# MAGIC 1. Removes the legacy configurations such as **HADOOP_USER_NAME, HADOOP_PROXY_USER**, add to the list if any other configs need to be removed

# COMMAND ----------

def callUpdateAPI(jobClusters,jobID):
  jobClusters["new_cluster"]["spark_version"] = "12.2.x-scala2.12"
  jobClusters["new_cluster"]["spark_conf"] = {"spark.databricks.sql.initial.catalog.name":"ucdev"}
  jobClusters["new_cluster"]["data_security_mode"] = "SINGLE_USER"
  #removes HADOOP_USER_NAME and HADOOP_PROXY_USER from spark conf, add here to the list if any other keys needs to be removed from spark conf
  jobClusters = remove_nested_keys(jobClusters, ['HADOOP_USER_NAME', 'HADOOP_PROXY_USER'])

  #below code builds the json to send as body to update jobs API
  data = {}
  new_settings = {}
  data['job_id'] = jobID
  new_settings['job_clusters'] = [jobClusters]
  data['new_settings'] = new_settings
  json_data = json.dumps(data, indent=3)

  url = f"https://{instancename}/api/2.1/jobs/update"

  jobUpdateResponse = req.post(url, headers=api_header, data=json_data)
  return jobUpdateResponse


# COMMAND ----------

def callJobsAPI(jobID):
  query_params = {"job_id": jobID}
  response = req.get(
      f"https://{instancename}/api/2.1/jobs/get",
      headers=api_header,
      params=query_params
  ).json()
  return response

# COMMAND ----------

def checkEligibility(jobClusters):
  sparkVersion = jobClusters["new_cluster"]["spark_version"]
  print(f"DBR version: {sparkVersion}")
  baseVersion = int(sparkVersion.split(".")[0])
  if baseVersion < 10:
    return False
  else:
    return True

# COMMAND ----------

def updateJob(jobID):
  print(f'Upgrading for job_id: {jobID}')
  response = callJobsAPI(jobID)
  jobClusters = response["settings"]["job_clusters"][0]
  jobs = str(jobClusters)
  #insert into table with existing job cluster configurations
  spark.sql(f"""insert into {tableToStoreJobs} values({jobID},"{jobs}")""")  

  if checkEligibility(jobClusters):
    jobUpdateResponse = callUpdateAPI(jobClusters,jobID)
    if jobUpdateResponse.status_code == 200:
      print(f"Updated successfully for job_id: {jobID}")
    else:
      print(f"Error while updating {jobID} : {jobUpdateResponse.content}")
  else:
    print(f"Upgrade not eligible for job id: {jobID}")

