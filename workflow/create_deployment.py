# create_deployment.py

from prefect.deployments import Deployment
from prefect.filesystems import GitHub
from .prefect_workflow import etl_pipeline

if __name__ == "__main__":
    deployment = Deployment.build_from_flow(
        flow=etl_pipeline,
        name="bank-marketing-etl",
        work_pool_name="my-managed-pool",  # Replace with your work pool name
        storage=GitHub(repository="your-repo/path"),  # Replace with your GitHub repo
    )
    deployment.apply()