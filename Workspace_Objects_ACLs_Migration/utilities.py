# Databricks notebook source
# DBTITLE 1,Importing required libraries
import requests as req
import pandas as pd
import json
import urllib.request
from pyspark.sql import functions as f
from pyspark.sql import types as t
import base64
import ast
from concurrent.futures import ProcessPoolExecutor
import multiprocessing
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.repos.api import ReposApi
from databricks_cli.workspace.api import WorkspaceApi
from functools import reduce

# COMMAND ----------

# DBTITLE 1,Getting the worker cores (Would be same as driver cores in case of single node cluster)
try:
  u = sc.uiWebUrl + "/api/v1/applications/" + sc.applicationId + "/allexecutors"
  with urllib.request.urlopen(u) as url:
      executors_data = json.loads(url.read().decode())
  worker_cores = [
      {"totalCores": i.get("totalCores")}
      for i in executors_data
      if i.get("id") != "driver"
  ][:1]
  if worker_cores == []:
      worker_cores = [{"totalCores": i.get("totalCores")} for i in executors_data]
  worker_cores = worker_cores[0]["totalCores"]
except:
  worker_cores = multiprocessing.cpu_count()
print("Worker cores: ", worker_cores)

# COMMAND ----------

# DBTITLE 1,Getting the driver cores
cores = multiprocessing.cpu_count()
print("Driver cores: ", cores)

# COMMAND ----------

# DBTITLE 1,Common API auth header for Databricks REST API calls
api_header = {"Authorization": f"Bearer {token}"}

# COMMAND ----------

# DBTITLE 1,URI dictionary for the different components
perm_uri_dict = {
    "Jobs": "https://{instancename}/api/2.0/preview/permissions/jobs/{artifact_id}",
    "Clusters": "https://{instancename}/api/2.0/preview/permissions/clusters/{artifact_id}",
    "Cluster_Policies": "https://{instancename}/api/2.0/preview/permissions/cluster-policies/{artifact_id}",
    "Pipelines": "https://{instancename}/api/2.0/preview/permissions/pipelines/{artifact_id}",
    "Pools": "https://{instancename}/api/2.0/preview/permissions/instance-pools/{artifact_id}",
    "SQL_Warehouses": "https://{instancename}/api/2.0/preview/permissions/sql/warehouses/{artifact_id}",
    #     "MLFlow_Experiments": "https://{instancename}/api/2.0/preview/permissions/experiments/{artifact_id}",
    "MLFlow_Models": "https://{instancename}/api/2.0/preview/permissions/registered-models/{artifact_id}",
    "Repos": "https://{instancename}/api/2.0/preview/permissions/repos/{artifact_id}",
    "Notebooks": "https://{instancename}/api/2.0/preview/permissions/notebooks/{artifact_id}",
    "Directories": "https://{instancename}/api/2.0/preview/permissions/directories/{artifact_id}",
    "Secrets": "https://{instancename}/api/2.0/secrets/acls/list/{scope}",
}

# COMMAND ----------

# DBTITLE 1,Category value dictionary for the different components
apply_category_dict = {
    "Jobs": "Job",
    "Clusters": "Cluster",
    "Cluster_Policies": "Cluster_Policy",
    "Pipelines": "Pipeline",
    "Pools": "Pool",
    "SQL_Warehouses": "SQL_Warehouse",
    #     "MLFlow_Experiments": "MLFlow_Experiment",
    "MLFlow_Models": "MLFlow_Model",
    "Repos": "Repo",
    "Notebooks": "Notebook",
    "Directories": "Directory",
    "Secrets": "Secret",
}

# COMMAND ----------

# DBTITLE 1,Functions for defining the transformations in each of the categories
def jobs_transformations(data, schema):
    perm_df = spark.createDataFrame(data, schema)
    perm_df = perm_df.withColumn("job_perms", f.explode(f.col("job_perms")))
    perm_df = perm_df.withColumn(
        "group_name", f.col("job_perms.group_name")
    ).withColumn("permissions", f.col("job_perms.permissions"))
    perm_df = perm_df.drop("job_perms")
    perm_df = perm_df.withColumn(
        "permissions", f.explode(f.col("permissions"))
    ).withColumnRenamed("job_id", "id")
    perm_df = perm_df.withColumn("artifact_type", f.lit("Job"))
    perm_df = perm_df.filter("name not like '%[RUNNER]%'")
    return perm_df


def clusters_transformations(data, schema):
    perm_df = spark.createDataFrame(data, schema)
    perm_df = perm_df.withColumn("cluster_perms", f.explode(f.col("cluster_perms")))
    perm_df = perm_df.withColumn(
        "group_name", f.col("cluster_perms.group_name")
    ).withColumn("permissions", f.col("cluster_perms.permissions"))
    perm_df = perm_df.drop("cluster_perms")
    perm_df = perm_df.withColumn(
        "permissions", f.explode(f.col("permissions"))
    ).withColumnRenamed("cluster_id", "id")
    perm_df = perm_df.withColumn("artifact_type", f.lit("Cluster"))
    perm_df = perm_df.filter("name not like 'job-%'")
    return perm_df


def cluster_policies_tranformations(data, schema):
    perm_df = spark.createDataFrame(data, schema)
    perm_df = perm_df.withColumn("policy_perms", f.explode(f.col("policy_perms")))
    perm_df = perm_df.withColumn(
        "group_name", f.col("policy_perms.group_name")
    ).withColumn("permissions", f.col("policy_perms.permissions"))
    perm_df = perm_df.drop("policy_perms")
    perm_df = perm_df.withColumn(
        "permissions", f.explode(f.col("permissions"))
    ).withColumnRenamed("policy_id", "id")
    perm_df = perm_df.withColumn("artifact_type", f.lit("Cluster_Policy"))
    return perm_df


def pipelines_transformations(data, schema):
    perm_df = spark.createDataFrame(data, schema)
    perm_df = perm_df.withColumn("pipeline_perms", f.explode(f.col("pipeline_perms")))
    perm_df = perm_df.withColumn(
        "group_name", f.col("pipeline_perms.group_name")
    ).withColumn("permissions", f.col("pipeline_perms.permissions"))
    perm_df = perm_df.drop("pipeline_perms")
    perm_df = perm_df.withColumn(
        "permissions", f.explode(f.col("permissions"))
    ).withColumnRenamed("pipeline_id", "id")
    perm_df = perm_df.withColumn("artifact_type", f.lit("Pipeline"))
    return perm_df


def pools_tranformations(data, schema):
    perm_df = spark.createDataFrame(data, schema)
    perm_df = perm_df.withColumn("pool_perms", f.explode(f.col("pool_perms")))
    perm_df = perm_df.withColumn(
        "group_name", f.col("pool_perms.group_name")
    ).withColumn("permissions", f.col("pool_perms.permissions"))
    perm_df = perm_df.drop("pool_perms")
    perm_df = perm_df.withColumn(
        "permissions", f.explode(f.col("permissions"))
    ).withColumnRenamed("instance_pool_id", "id")
    perm_df = perm_df.withColumn("artifact_type", f.lit("Pool"))
    return perm_df


def sql_warehouses_tranformations(data, schema):
    perm_df = spark.createDataFrame(data, schema)
    perm_df = perm_df.withColumn("warehouse_perms", f.explode(f.col("warehouse_perms")))
    perm_df = perm_df.withColumn(
        "group_name", f.col("warehouse_perms.group_name")
    ).withColumn("permissions", f.col("warehouse_perms.permissions"))
    perm_df = perm_df.drop("warehouse_perms")
    perm_df = perm_df.withColumn(
        "permissions", f.explode(f.col("permissions"))
    ).withColumnRenamed("warehouse_id", "id")
    perm_df = perm_df.withColumn("artifact_type", f.lit("SQL_Warehouse"))
    return perm_df


# def mlflow_experiments_transformations(data,schema):
#     perm_df = spark.createDataFrame(data, schema)
#     perm_df = perm_df.withColumn("experiment_perms", f.explode(f.col("experiment_perms")))
#     perm_df = perm_df.withColumn(
#         "group_name", f.col("experiment_perms.group_name")
#     ).withColumn("permissions", f.col("experiment_perms.permissions"))
#     perm_df = perm_df.drop("experiment_perms")
#     perm_df = perm_df.withColumn(
#         "permissions", f.explode(f.col("permissions"))
#     ).withColumnRenamed("experiment_id", "id")
#     perm_df = perm_df.withColumn("artifact_type", f.lit("MLFlow_Experiment"))
#     return perm_df


def mlflow_models_transformations(data, schema):
    perm_df = spark.createDataFrame(data, schema)
    perm_df = perm_df.withColumn("model_perms", f.explode(f.col("model_perms")))
    perm_df = perm_df.withColumn(
        "group_name", f.col("model_perms.group_name")
    ).withColumn("permissions", f.col("model_perms.permissions"))
    perm_df = perm_df.drop("model_perms")
    perm_df = perm_df.withColumn(
        "permissions", f.explode(f.col("permissions"))
    ).withColumnRenamed("model_id", "id")
    perm_df = perm_df.withColumn("artifact_type", f.lit("MLFlow_Model"))
    return perm_df


def repos_tranformations(data, schema):
    perm_df = spark.createDataFrame(data, schema)
    perm_df = perm_df.withColumn("repo_perms", f.explode(f.col("repo_perms")))
    perm_df = perm_df.withColumn(
        "group_name", f.col("repo_perms.group_name")
    ).withColumn("permissions", f.col("repo_perms.permissions"))
    perm_df = perm_df.drop("repo_perms")
    perm_df = perm_df.withColumn(
        "permissions", f.explode(f.col("permissions"))
    ).withColumnRenamed("repo_id", "id")
    perm_df = perm_df.withColumn("artifact_type", f.lit("Repo"))
    return perm_df


def notebooks_tranformations(data, schema):
    perm_df = spark.createDataFrame(data, schema)
    perm_df = perm_df.withColumn("notebook_perms", f.explode(f.col("notebook_perms")))
    perm_df = perm_df.withColumn(
        "group_name", f.col("notebook_perms.group_name")
    ).withColumn("permissions", f.col("notebook_perms.permissions"))
    perm_df = perm_df.drop("notebook_perms")
    perm_df = perm_df.withColumn(
        "permissions", f.explode(f.col("permissions"))
    ).withColumnRenamed("notebook_id", "id")
    perm_df = perm_df.withColumn("artifact_type", f.lit("Notebook"))
    return perm_df


def directories_tranformations(data, schema):
    perm_df = spark.createDataFrame(data, schema)
    perm_df = perm_df.withColumn("directory_perms", f.explode(f.col("directory_perms")))
    perm_df = perm_df.withColumn(
        "group_name", f.col("directory_perms.group_name")
    ).withColumn("permissions", f.col("directory_perms.permissions"))
    perm_df = perm_df.drop("directory_perms")
    perm_df = perm_df.withColumn(
        "permissions", f.explode(f.col("permissions"))
    ).withColumnRenamed("directory_id", "id")
    perm_df = perm_df.withColumn("artifact_type", f.lit("Directory"))
    return perm_df

def secrets_tranformations(data, schema):
    perm_df = spark.createDataFrame(data, schema)
    perm_df = perm_df.withColumn("scope_perms", f.explode(f.col("scope_perms")))
    perm_df = perm_df.withColumn(
        "group_name", f.col("scope_perms.group_name")
    ).withColumn("permissions", f.col("scope_perms.permissions"))
    perm_df = perm_df.drop("scope_perms")
    perm_df = perm_df.withColumn("artifact_type", f.lit("Secret"))
    return perm_df

# COMMAND ----------

# DBTITLE 1,Method for getting the model ID for a model name
def get_model_id(name):
    query_params = {"name": name}
    model_call = req.get(
        f"https://{instancename}/api/2.0/mlflow/databricks/registered-models/get",
        headers=api_header,
        params=query_params,
    ).json()
    model_detail = model_call["registered_model_databricks"]
    return model_detail["id"]

# COMMAND ----------

# DBTITLE 1,Method for getting secrets permissions
def get_secrets_acls(name):
    query_params = {"scope": name}
    try:
      secrets_call = req.get(
        f"https://{instancename}/api/2.0/secrets/acls/list",
        headers=api_header,
        params=query_params,
    ).json()

      secrets_detail = secrets_call["items"]
      if secrets_detail != None:
        filt_secrets_detail = [i for i in secrets_detail if "principal" in i.keys()]
        filt_secrets_detail_parsed = [
            {
                "group_name": i.get("principal"),
                "permissions": i.get("permission")
            }
            for i in filt_secrets_detail
        ]
      return filt_secrets_detail_parsed
    except:
      return [None]

# COMMAND ----------

# DBTITLE 1,Getting the 
def get_pipelines():
    pipeline_lst = []

    query_params = {"max_results": 100}
    response = req.get(
        f"https://{instancename}/api/2.0/pipelines",
        headers=api_header,
        params=query_params,
    ).json()
    has_more = response.get("next_page_token")

    pipeline_lst = pipeline_lst + response.get("statuses")

    while has_more:
        query_params["page_token"] = has_more
        recurse_response = req.get(
            f"https://{instancename}/api/2.0/pipelines",
            headers=api_header,
            params=query_params,
        ).json()
        pipeline_lst = pipeline_lst + recurse_response.get("statuses")
        has_more = recurse_response.get("next_page_token")
    pipeline_lst = [
        {"name": i.get("name"), "pipeline_id": i.get("pipeline_id")}
        for i in pipeline_lst
    ]
    print("Pipelines count:", len(pipeline_lst))
    return pipeline_lst


def get_cluster_policies():
    cluster_policy_response = req.get(
        f"https://{instancename}/api/2.0/policies/clusters/list", headers=api_header
    ).json()
    cluster_policy_response
    cluster_policy_list = [
        {"name": i.get("name"), "policy_id": i.get("policy_id")}
        for i in cluster_policy_response["policies"]
    ]
    print("Cluster policy count:", len(cluster_policy_list))
    return cluster_policy_list


def get_clusters():
    clusters_response = req.get(
        f"https://{instancename}/api/2.0/clusters/list", headers=api_header
    ).json()
    clusters_list = [
        {"name": i.get("cluster_name"), "cluster_id": i.get("cluster_id")}
        for i in clusters_response["clusters"]
    ]
    print("Clusters count:", len(clusters_list))
    return clusters_list


def get_jobs():
    job_lst = []

    query_params = {"limit": 25, "offset": 0, "expand_tasks": "false"}
    response = req.get(
        f"https://{instancename}/api/2.1/jobs/list?attributes=job_id",
        headers=api_header,
        params=query_params,
    ).json()

    has_more = response["has_more"]

    job_lst = job_lst + response["jobs"]

    while has_more:
        query_params["offset"] = query_params["offset"] + query_params["limit"]
        recurse_response = req.get(
            f"https://{instancename}/api/2.1/jobs/list?attributes=job_id",
            headers=api_header,
            params=query_params,
        ).json()
        job_lst = job_lst + recurse_response["jobs"]
        has_more = recurse_response["has_more"]
    job_lst = [
        {"name": i.get("settings")["name"], "job_id": i.get("job_id")} for i in job_lst
    ]
    print("Job List count:", len(job_lst))
    return job_lst


def get_pools():
    pools_response = req.get(
        f"https://{instancename}/api/2.0/instance-pools/list", headers=api_header
    ).json()
    pools_list = [
        {
            "name": i.get("instance_pool_name"),
            "instance_pool_id": i.get("instance_pool_id"),
        }
        for i in pools_response["instance_pools"]
    ]
    print("Pools list count:", len(pools_list))
    return pools_list


def get_warehouses():
    warehouses_response = req.get(
        f"https://{instancename}/api/2.0/sql/warehouses/", headers=api_header
    ).json()
    #print(warehouses_response)
    warehouses_response["warehouses"]
    warehouses_list = [
        {"name": i.get("name"), "warehouse_id": i.get("id")}
        for i in warehouses_response["warehouses"]
    ]
    print("Warehouses count:", len(warehouses_list))
    return warehouses_list


# def get_experiments():
#     experiment_lst = []

#     query_params = {"max_results": 500}
#     response = req.get(
#         f"https://{instancename}/api/2.0/mlflow/experiments/search",
#         headers=api_header,
#         params=query_params,
#     ).json()

#     has_more = response.get("next_page_token")

#     experiment_lst = experiment_lst + response["experiments"]

#     while has_more:
#         query_params["page_token"] = has_more
#         recurse_response = req.get(
#             f"https://{instancename}/api/2.0/mlflow/experiments/search",
#             headers=api_header,
#             params=query_params,
#         ).json()
#         experiment_lst = experiment_lst + recurse_response["experiments"]
#         has_more = recurse_response.get("next_page_token")
#     experiment_lst = [{"experiment_id": i.get("experiment_id")} for i in experiment_lst]
#     print("Experiment List count:", len(experiment_lst))
#     return experiment_lst


def get_models():
    model_lst = []

    query_params = {"max_results": 500}
    response = req.get(
        f"https://{instancename}/api/2.0/mlflow/registered-models/search",
        headers=api_header,
        params=query_params,
    ).json()

    has_more = response.get("next_page_token")

    model_lst = model_lst + response["registered_models"]

    while has_more:
        query_params["page_token"] = has_more
        recurse_response = req.get(
            f"https://{instancename}/api/2.0/mlflow/registered-models/search",
            headers=api_header,
            params=query_params,
        ).json()
        model_lst = model_lst + recurse_response["registered_models"]
        has_more = recurse_response.get("next_page_token")
    model_lst = [{"model_name": i.get("name")} for i in model_lst]
    model_lst = [
        {"name": i["model_name"], "model_id": get_model_id(i["model_name"])}
        for i in model_lst
    ]
    print("Model List count:", len(model_lst))
    return model_lst


def get_repos():
    api_client = ApiClient(host=f"https://{instancename}", token=token)
    repos_api = ReposApi(api_client)
    repos = list()

    repo_resp = repos_api.list("/Repos/", next_page_token=None)
    next_token = repo_resp.get("next_page_token")
    repos.append(repo_resp["repos"])

    while next_token:
        repo_resp_recur = repos_api.list("/Repos/", next_page_token=next_token)
        next_token = repo_resp_recur.get("next_page_token")
        repos.append(repo_resp_recur["repos"])
    repos = reduce(lambda x, y: x + y, repos)
    repos = [{"name": i.get("path"), "repo_id": i.get("id")} for i in repos]
    print("Repos List count:", len(repos))
    return repos


def get_notebooks():
    api_client = ApiClient(host=f"https://{instancename}", token=token)
    workspace_api = WorkspaceApi(api_client)
    directories = []
    notebooks = []

    def list_workspace_objects(path="/"):
        elements = [
            {
                "path": i.path,
                "object_type": i.object_type,
                "language": i.language,
                "object_id": i.object_id,
            }
            for i in workspace_api.list_objects(path)
            if i.path.startswith("/Repos") != True
        ]
        if elements is not None:
            for object in elements:
                if object.get("object_type") == "NOTEBOOK":
                    notebooks.append(
                        {
                            "name": object.get("path"),
                            "notebook_id": object.get("object_id"),
                        }
                    )
                if object.get("object_type") == "DIRECTORY":
                    directories.append(
                        {
                            "name": object.get("path"),
                            "directory_id": object.get("object_id"),
                        }
                    )
                    list_workspace_objects(path=object.get("path"))

    list_workspace_objects("/")
    print("Notebooks List count:", len(notebooks))
    return notebooks


def get_directories():
    api_client = ApiClient(host=f"https://{instancename}", token=token)
    workspace_api = WorkspaceApi(api_client)
    directories = []
    notebooks = []

    def list_workspace_objects(path="/"):
        elements = [
            {
                "path": i.path,
                "object_type": i.object_type,
                "language": i.language,
                "object_id": i.object_id,
            }
            for i in workspace_api.list_objects(path)
            if i.path.startswith("/Repos") != True
        ]
        if elements is not None:
            for object in elements:
                if object.get("object_type") == "NOTEBOOK":
                    notebooks.append(
                        {
                            "name": object.get("path"),
                            "notebook_id": object.get("object_id"),
                        }
                    )
                if object.get("object_type") == "DIRECTORY":
                    directories.append(
                        {
                            "name": object.get("path"),
                            "directory_id": object.get("object_id"),
                        }
                    )
                    list_workspace_objects(path=object.get("path"))

    list_workspace_objects("/")
    print("Directories List count:", len(directories))
    return directories
  
def get_secrets():

  secrets_response = req.get(
        f"https://{instancename}/api/2.0/secrets/scopes/list",
        headers=api_header
    ).json()

  secrets_lst = [
        {"id": i.get("name"), "name": i.get("name")} for i in secrets_response["scopes"]
    ]
  print("Secrets count:", len(secrets_lst))
  return secrets_lst

# COMMAND ----------

def get_grp_df():
    grp_uri = f"https://{instancename}/api/2.0/preview/scim/v2/Groups?attributes=displayName,meta,entitlements"
    all_grps = req.get(grp_uri, headers=api_header)
    data = all_grps.json()["Resources"]
    grp_df = spark.createDataFrame(data)
    grp_df = grp_df.withColumn(
        "uc_group_name", f.col("displayName")).selectExpr("id", "displayName as group_name", "uc_group_name")
    if (type(groups_of_interest) == list) and (len(groups_of_interest) > 0):
        grp_df = grp_df.filter(f.col("group_name").isin(groups_of_interest))
    elif (type(groups_of_interest) == str) and (len(groups_of_interest) > 0):
        grp_df = grp_df.filter(f.col("group_name") == groups_of_interest)
    else:
        pass
    return grp_df

# COMMAND ----------

def get_permissions(uri):
    print(uri)
    perm_call = req.get(
        uri,
        headers=api_header,
    ).json()
    perm_list = perm_call.get("access_control_list")
    if perm_list != None:
        filt_perm_list = [i for i in perm_list if "group_name" in i.keys()]
        filt_perm_list_parsed = [
            {
                "group_name": i["group_name"],
                "permissions": [j["permission_level"] for j in i["all_permissions"]],
            }
            for i in filt_perm_list
        ]
        print(filt_perm_list_parsed)
        return filt_perm_list_parsed
    else:
        return [None]

# COMMAND ----------

def parse_artifact_list(type_of_permission_migration, artifact_list):
    if type_of_permission_migration == "Jobs":
        job_perms_group_perms = [
            {
                "name": i["name"],
                "job_id": i["job_id"],
                "uri": perm_uri.format(
                    instancename=instancename, artifact_id=i["job_id"]
                ),
            }
            for i in artifact_list
        ]
        job_perms_group_perms_simple = [i["uri"] for i in job_perms_group_perms]
        result = []
        with ProcessPoolExecutor(max_workers=cores) as executor:
            for r in executor.map(get_permissions, job_perms_group_perms_simple):
                result.append(r)
        zip_up = list(zip(job_perms_group_perms, result))
        return_job_perms = [
            {"name": i[0]["name"], "job_id": i[0]["job_id"], "job_perms": i[1]}
            for i in zip_up
        ]
        return return_job_perms

    elif type_of_permission_migration == "Clusters":
        clusters_perms_group_perms = [
            {
                "name": i["name"],
                "cluster_id": i["cluster_id"],
                "uri": perm_uri.format(
                    instancename=instancename, artifact_id=i["cluster_id"]
                ),
            }
            for i in artifact_list
        ]
        clusters_perms_group_perms_simple = [
            i["uri"] for i in clusters_perms_group_perms
        ]
        result = []
        with ProcessPoolExecutor(max_workers=cores) as executor:
            for r in executor.map(get_permissions, clusters_perms_group_perms_simple):
                result.append(r)
        zip_up = list(zip(clusters_perms_group_perms, result))
        return_cluster_perms = [
            {
                "name": i[0]["name"],
                "cluster_id": i[0]["cluster_id"],
                "cluster_perms": i[1],
            }
            for i in zip_up
        ]
        return return_cluster_perms

    elif type_of_permission_migration == "Cluster_Policies":
        policy_perms_group_perms = [
            {
                "name": i["name"],
                "policy_id": i["policy_id"],
                "uri": perm_uri.format(
                    instancename=instancename, artifact_id=i["policy_id"]
                ),
            }
            for i in artifact_list
        ]
        policy_perms_group_perms_simple = [i["uri"] for i in policy_perms_group_perms]
        result = []
        with ProcessPoolExecutor(max_workers=cores) as executor:
            for r in executor.map(get_permissions, policy_perms_group_perms_simple):
                result.append(r)
        zip_up = list(zip(policy_perms_group_perms, result))
        return_policy_perms = [
            {"name": i[0]["name"], "policy_id": i[0]["policy_id"], "policy_perms": i[1]}
            for i in zip_up
        ]
        return return_policy_perms

    elif type_of_permission_migration == "Pipelines":
        pipeline_perms_group_perms = [
            {
                "name": i["name"],
                "pipeline_id": i["pipeline_id"],
                "uri": perm_uri.format(
                    instancename=instancename, artifact_id=i["pipeline_id"]
                ),
            }
            for i in artifact_list
        ]
        pipeline_perms_group_perms_simple = [
            i["uri"] for i in pipeline_perms_group_perms
        ]
        result = []
        with ProcessPoolExecutor(max_workers=cores) as executor:
            for r in executor.map(get_permissions, pipeline_perms_group_perms_simple):
                result.append(r)
        zip_up = list(zip(pipeline_perms_group_perms, result))
        return_pipeline_perms = [
            {
                "name": i[0]["name"],
                "pipeline_id": i[0]["pipeline_id"],
                "pipeline_perms": i[1],
            }
            for i in zip_up
        ]
        return return_pipeline_perms

    elif type_of_permission_migration == "Pools":
        pool_perms_group_perms = [
            {
                "name": i["name"],
                "instance_pool_id": i["instance_pool_id"],
                "uri": perm_uri.format(
                    instancename=instancename, artifact_id=i["instance_pool_id"]
                ),
            }
            for i in artifact_list
        ]
        pool_perms_group_perms_simple = [i["uri"] for i in pool_perms_group_perms]
        result = []
        with ProcessPoolExecutor(max_workers=cores) as executor:
            for r in executor.map(get_permissions, pool_perms_group_perms_simple):
                result.append(r)
        zip_up = list(zip(pool_perms_group_perms, result))
        return_pool_perms = [
            {
                "name": i[0]["name"],
                "instance_pool_id": i[0]["instance_pool_id"],
                "pool_perms": i[1],
            }
            for i in zip_up
        ]
        return return_pool_perms

    elif type_of_permission_migration == "SQL_Warehouses":
        warehouse_perms_group_perms = [
            {
                "name": i["name"],
                "warehouse_id": i["warehouse_id"],
                "uri": perm_uri.format(
                    instancename=instancename, artifact_id=i["warehouse_id"]
                ),
            }
            for i in artifact_list
        ]
        warehouse_perms_group_perms_simple = [
            i["uri"] for i in warehouse_perms_group_perms
        ]
        result = []
        with ProcessPoolExecutor(max_workers=cores) as executor:
            for r in executor.map(get_permissions, warehouse_perms_group_perms_simple):
                result.append(r)
        zip_up = list(zip(warehouse_perms_group_perms, result))
        print(zip_up)
        return_warehouse_perms = [
            {
                "name": i[0]["name"],
                "warehouse_id": i[0]["warehouse_id"],
                "warehouse_perms": i[1],
            }
            for i in zip_up
        ]
        return return_warehouse_perms

    #   elif type_of_permission_migration == "MLFlow_Experiments":
    #     experiment_perms_group_perms = [{'experiment_id':i["experiment_id"],'uri':perm_uri.format(instancename=instancename, artifact_id=i["experiment_id"])} for i in artifact_list]
    #     experiment_perms_group_perms_simple = [i['uri'] for i in experiment_perms_group_perms]
    #     result = []
    #     with ProcessPoolExecutor(max_workers=cores) as executor:
    #         for r in executor.map(get_permissions, experiment_perms_group_perms_simple):
    #             result.append(r)
    #     zip_up = list(zip(experiment_perms_group_perms,result))
    #     return_experiment_perms = [{'experiment_id':i[0]['experiment_id'],'experiment_perms':i[1]} for i in zip_up]
    #     return return_experiment_perms

    elif type_of_permission_migration == "MLFlow_Models":
        model_perms_group_perms = [
            {
                "name": i["name"],
                "model_id": i["model_id"],
                "uri": perm_uri.format(
                    instancename=instancename, artifact_id=i["model_id"]
                ),
            }
            for i in artifact_list
        ]
        model_perms_group_perms_simple = [i["uri"] for i in model_perms_group_perms]
        result = []
        with ProcessPoolExecutor(max_workers=cores) as executor:
            for r in executor.map(get_permissions, model_perms_group_perms_simple):
                result.append(r)
        zip_up = list(zip(model_perms_group_perms, result))
        return_experiment_perms = [
            {"name": i[0]["name"], "model_id": i[0]["model_id"], "model_perms": i[1]}
            for i in zip_up
        ]
        return return_experiment_perms

    elif type_of_permission_migration == "Repos":
        repo_perms_group_perms = [
            {
                "name": i["name"],
                "repo_id": i["repo_id"],
                "uri": perm_uri.format(
                    instancename=instancename, artifact_id=i["repo_id"]
                ),
            }
            for i in artifact_list
        ]
        repo_perms_group_perms_simple = [i["uri"] for i in repo_perms_group_perms]
        result = []
        with ProcessPoolExecutor(max_workers=cores) as executor:
            for r in executor.map(get_permissions, repo_perms_group_perms_simple):
                result.append(r)
        zip_up = list(zip(repo_perms_group_perms, result))
        return_repo_perms = [
            {"name": i[0]["name"], "repo_id": i[0]["repo_id"], "repo_perms": i[1]}
            for i in zip_up
        ]
        return return_repo_perms

    elif type_of_permission_migration == "Notebooks":
        notebook_perms_group_perms = [
            {
                "name": i["name"],
                "notebook_id": i["notebook_id"],
                "uri": perm_uri.format(
                    instancename=instancename, artifact_id=i["notebook_id"]
                ),
            }
            for i in artifact_list
        ]
        notebook_perms_group_perms_simple = [
            i["uri"] for i in notebook_perms_group_perms
        ]
        result = []
        with ProcessPoolExecutor(max_workers=cores) as executor:
            for r in executor.map(get_permissions, notebook_perms_group_perms_simple):
                result.append(r)
        zip_up = list(zip(notebook_perms_group_perms, result))
        return_notebook_perms = [
            {
                "name": i[0]["name"],
                "notebook_id": i[0]["notebook_id"],
                "notebook_perms": i[1],
            }
            for i in zip_up
        ]
        return return_notebook_perms

    elif type_of_permission_migration == "Directories":
        directory_perms_group_perms = [
            {
                "name": i["name"],
                "directory_id": i["directory_id"],
                "uri": perm_uri.format(
                    instancename=instancename, artifact_id=i["directory_id"]
                ),
            }
            for i in artifact_list
        ]
        directory_perms_group_perms_simple = [
            i["uri"] for i in directory_perms_group_perms
        ]
        result = []
        with ProcessPoolExecutor(max_workers=cores) as executor:
            for r in executor.map(get_permissions, directory_perms_group_perms_simple):
                result.append(r)
        print(result)
        zip_up = list(zip(directory_perms_group_perms, result))
        return_directory_perms = [
            {
                "name": i[0]["name"],
                "directory_id": i[0]["directory_id"],
                "directory_perms": i[1],
            }
            for i in zip_up
        ]
        return return_directory_perms
    elif type_of_permission_migration == "Secrets":
        secrets_perms_group_perms = [
            {
                "id": i["id"],
                "name": i["name"],
                "uri": perm_uri.format(
                    instancename=instancename, scope=i["name"]
                ),
            }
            for i in artifact_list
        ]
        secrets_perms_group_perms_simple = [
            i["id"] for i in secrets_perms_group_perms
        ]
        result = []
        with ProcessPoolExecutor(max_workers=3) as executor:
            for r in executor.map(get_secrets_acls, secrets_perms_group_perms_simple):
                result.append(r)
        zip_up = list(zip(secrets_perms_group_perms, result))
        return_secrets_perms = [
            {
                "id": i[0]["id"],
                "name": i[0]["id"],
                "scope_perms": i[1],
            }
            for i in zip_up
        ]
        return return_secrets_perms

# COMMAND ----------

perm_tranformations_func_dict = {
    "Jobs": jobs_transformations,
    "Clusters": clusters_transformations,
    "Cluster_Policies": cluster_policies_tranformations,
    "Pipelines": pipelines_transformations,
    "Pools": pools_tranformations,
    "SQL_Warehouses": sql_warehouses_tranformations,
    #     "MLFlow_Experiments": mlflow_experiments_transformations,
    "MLFlow_Models": mlflow_models_transformations,
    "Repos": repos_tranformations,
    "Notebooks": notebooks_tranformations,
    "Directories": directories_tranformations,
    "Secrets": secrets_tranformations,
}

# COMMAND ----------

perm_data_dict = {
    "Jobs": get_jobs,
    "Clusters": get_clusters,
    "Cluster_Policies": get_cluster_policies,
    "Pipelines": get_pipelines,
    "Pools": get_pools,
    "SQL_Warehouses": get_warehouses,
    #     "MLFlow_Experiments": get_experiments,
    "MLFlow_Models": get_models,
    "Repos": get_repos,
    "Notebooks": get_notebooks,
    "Directories": get_directories,
    "Secrets": get_secrets,
}

# COMMAND ----------

schema_dict = {
    "Jobs": t.StructType(
        [
            t.StructField("name", t.StringType()),
            t.StructField("job_id", t.StringType()),
            t.StructField(
                "job_perms",
                t.ArrayType(
                    t.StructType(
                        [
                            t.StructField("group_name", t.StringType()),
                            t.StructField("permissions", t.ArrayType(t.StringType())),
                        ]
                    )
                ),
            ),
        ]
    ),
    "Clusters": t.StructType(
        [
            t.StructField("name", t.StringType()),
            t.StructField("cluster_id", t.StringType()),
            t.StructField(
                "cluster_perms",
                t.ArrayType(
                    t.StructType(
                        [
                            t.StructField("group_name", t.StringType()),
                            t.StructField("permissions", t.ArrayType(t.StringType())),
                        ]
                    )
                ),
            ),
        ]
    ),
    "Cluster_Policies": t.StructType(
        [
            t.StructField("name", t.StringType()),
            t.StructField("policy_id", t.StringType()),
            t.StructField(
                "policy_perms",
                t.ArrayType(
                    t.StructType(
                        [
                            t.StructField("group_name", t.StringType()),
                            t.StructField("permissions", t.ArrayType(t.StringType())),
                        ]
                    )
                ),
            ),
        ]
    ),
    "Pipelines": t.StructType(
        [
            t.StructField("name", t.StringType()),
            t.StructField("pipeline_id", t.StringType()),
            t.StructField(
                "pipeline_perms",
                t.ArrayType(
                    t.StructType(
                        [
                            t.StructField("group_name", t.StringType()),
                            t.StructField("permissions", t.ArrayType(t.StringType())),
                        ]
                    )
                ),
            ),
        ]
    ),
    "Pools": t.StructType(
        [
            t.StructField("name", t.StringType()),
            t.StructField("instance_pool_id", t.StringType()),
            t.StructField(
                "pool_perms",
                t.ArrayType(
                    t.StructType(
                        [
                            t.StructField("group_name", t.StringType()),
                            t.StructField("permissions", t.ArrayType(t.StringType())),
                        ]
                    )
                ),
            ),
        ]
    ),
    "SQL_Warehouses": t.StructType(
        [
            t.StructField("name", t.StringType()),
            t.StructField("warehouse_id", t.StringType()),
            t.StructField(
                "warehouse_perms",
                t.ArrayType(
                    t.StructType(
                        [
                            t.StructField("group_name", t.StringType()),
                            t.StructField("permissions", t.ArrayType(t.StringType())),
                        ]
                    )
                ),
            ),
        ]
    ),
    #     "MLFlow_Experiments": t.StructType(
    #         [
    #             t.StructField("experiment_id", t.StringType()),
    #             t.StructField(
    #                 "experiment_perms",
    #                 t.ArrayType(
    #                     t.StructType(
    #                         [
    #                             t.StructField("group_name", t.StringType()),
    #                             t.StructField("permissions", t.ArrayType(t.StringType())),
    #                         ]
    #                     )
    #                 ),
    #             ),
    #         ]
    #     ),
    "MLFlow_Models": t.StructType(
        [
            t.StructField("name", t.StringType()),
            t.StructField("model_id", t.StringType()),
            t.StructField(
                "model_perms",
                t.ArrayType(
                    t.StructType(
                        [
                            t.StructField("group_name", t.StringType()),
                            t.StructField("permissions", t.ArrayType(t.StringType())),
                        ]
                    )
                ),
            ),
        ]
    ),
    "Repos": t.StructType(
        [
            t.StructField("name", t.StringType()),
            t.StructField("repo_id", t.StringType()),
            t.StructField(
                "repo_perms",
                t.ArrayType(
                    t.StructType(
                        [
                            t.StructField("group_name", t.StringType()),
                            t.StructField("permissions", t.ArrayType(t.StringType())),
                        ]
                    )
                ),
            ),
        ]
    ),
    "Notebooks": t.StructType(
        [
            t.StructField("name", t.StringType()),
            t.StructField("notebook_id", t.StringType()),
            t.StructField(
                "notebook_perms",
                t.ArrayType(
                    t.StructType(
                        [
                            t.StructField("group_name", t.StringType()),
                            t.StructField("permissions", t.ArrayType(t.StringType())),
                        ]
                    )
                ),
            ),
        ]
    ),
    "Directories": t.StructType(
        [
            t.StructField("name", t.StringType()),
            t.StructField("directory_id", t.StringType()),
            t.StructField(
                "directory_perms",
                t.ArrayType(
                    t.StructType(
                        [
                            t.StructField("group_name", t.StringType()),
                            t.StructField("permissions", t.ArrayType(t.StringType())),
                        ]
                    )
                ),
            ),
        ]
    ),
    "Secrets": t.StructType(
        [
            t.StructField("id", t.StringType()),
            t.StructField("name", t.StringType()),
            t.StructField(
                "scope_perms",
                t.ArrayType(
                    t.StructType(
                        [
                            t.StructField("group_name", t.StringType()),
                            t.StructField("permissions", t.StringType()),
                        ]
                    )
                ),
            ),
        ]
    ),
}

# COMMAND ----------


