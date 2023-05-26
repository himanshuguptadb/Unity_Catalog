# Databricks notebook source
# MAGIC %run ./upgrade_jobs_util

# COMMAND ----------

import unittest
import json

# COMMAND ----------

jobCLuster = """{"job_cluster_key": "Job_cluster", "new_cluster": {"cluster_name": "", "spark_version": "7.3.x-scala2.12", "spark_conf": {"spark.databricks.sql.initial.catalog.name": "ucdev"}, "aws_attributes": {"first_on_demand": 1, "availability": "SPOT_WITH_FALLBACK", "zone_id": "us-west-2a", "spot_bid_price_percent": 100, "ebs_volume_count": 0}, "node_type_id": "i3.xlarge", "spark_env_vars": {"LD_LIBRARY_PATH": "/tmp/eFPA/"}, "data_security_mode": "LEGACY_SINGLE_USER_STANDARD", "runtime_engine": "STANDARD", "num_workers": 1}}"""

res = json.loads(jobCLuster)

assert checkEligibility(res) == False
print('Test Passed')

# COMMAND ----------

jobCLuster = """{"job_cluster_key": "Job_cluster", "new_cluster": {"cluster_name": "", "spark_version": "10.4.x-scala2.12", "spark_conf": {"spark.databricks.sql.initial.catalog.name": "ucdev"}, "aws_attributes": {"first_on_demand": 1, "availability": "SPOT_WITH_FALLBACK", "zone_id": "us-west-2a", "spot_bid_price_percent": 100, "ebs_volume_count": 0}, "node_type_id": "i3.xlarge", "spark_env_vars": {"LD_LIBRARY_PATH": "/tmp/eFPA/"}, "data_security_mode": "LEGACY_SINGLE_USER_STANDARD", "runtime_engine": "STANDARD", "num_workers": 1}}"""

res = json.loads(jobCLuster)

assert checkEligibility(res) == True
print('Test Passed')
