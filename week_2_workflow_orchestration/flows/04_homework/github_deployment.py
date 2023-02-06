from prefect.deployments import Deployment
from etl_web_to_gcs import etl_main_flow
from prefect.filesystems import GitHub 

storage = GitHub.load("zoom-github")

deployment = Deployment.build_from_flow(
     flow=etl_main_flow,
     name="gh-storage_flow",
     storage=storage,
     entrypoint="week_2_workflow_orchestration/flows/04_homework/etl_web_to_gcs.py:etl_main_flow")

if __name__ == "__main__":
    deployment.apply()

