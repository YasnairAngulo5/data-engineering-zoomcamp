from prefect import Flow


if __name__ == '__main__':

    flow = Flow("etl_parent_flow")
    flow.register(project_name="Prefect zoom", storage="zoom-github")
    flow.run()

prefect deployment build ./week_2_workflow_orchestration/homework/flows/etl_web_to_gcs.py:etl_parent_flow \
  -n web-to-gcs-gh-etl\
  -sb "github/zoom-github" \
  --apply


week_2_workflow_orchestration/homework/flows/
https://github.com/yasnair/data-engineering-zoomcamp-1.git


week_2_workflow_orchestration/homework
