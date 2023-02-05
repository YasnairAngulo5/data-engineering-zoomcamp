from prefect.deployments import Deployment
from etl_web_to_gcs_Q4 import etl_web_to_gcs
from prefect.filesystems import GitHub 

storage = GitHub.load("zoom-github")

deployment = Deployment.build_from_flow(
     flow=etl_web_to_gcs,
     name="web-to-gcs-gh-etl",
     storage=storage,
     entrypoint="week_2_workflow_orchestration/homework/flows/etl_web_to_gcs.py:etl_parent_flow")

if __name__ == "__main__":
    deployment.apply()
