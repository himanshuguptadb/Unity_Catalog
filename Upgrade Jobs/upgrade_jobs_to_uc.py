# Databricks notebook source
# MAGIC %run ./upgrade_jobs_util

# COMMAND ----------

# This is one time activity, please run this outside of this notebook if needed
# spark.sql(f"""create table {tableToStoreJobs}(
#                       job_id bigint,
#                       cluster_config string
#                   ) 
#           """)

# COMMAND ----------

dbutils.widgets.text("tableToStoreJobs", "srikanth_anumula_catalog. srikanth_uc_migrate.job_config")
tableToStoreJobs = dbutils.widgets.get("tableToStoreJobs")
print(tableToStoreJobs)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Below code to fetch list of job ids in a workspace
# MAGIC
# MAGIC **Note:** Uncomment below code to upgrade all jobs in a workspace, use with caution

# COMMAND ----------

# job_lst = []
# job_id_lst = []

# query_params = {"limit": 20, "offset": 0, "expand_tasks": "false"}
# response = req.get(
#     f"https://{instancename}/api/2.1/jobs/list?attributes=job_id",
#     headers=api_header,
#     params=query_params,
# ).json()

# has_more = response["has_more"]

# job_lst = job_lst + response["jobs"]
# job_id_lst = [
#     i.get("job_id") for i in job_lst
# ]

# while has_more:
#     query_params["offset"] = query_params["offset"] + query_params["limit"]
#     recurse_response = req.get(
#         f"https://{instancename}/api/2.1/jobs/list?attributes=job_id",
#         headers=api_header,
#         params=query_params,
#     ).json()
#     job_lst = job_lst + recurse_response["jobs"]
#     has_more = recurse_response["has_more"]

# job_id_lst = [
#     i.get("job_id") for i in job_lst
# ]
# print("Job List count:", len(job_id_lst))
# print(job_id_lst)

# COMMAND ----------

#uncomment below step to upgade all jobs, include specified job list here to upgrade

job_id_lst = [101282017860824]
for jobID in job_id_lst:
  updateJob(jobID)
