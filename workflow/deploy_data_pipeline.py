from prefect import flow

if __name__ == "__main__":
    flow.from_source(
        source="/workflow/data_processing_flow.py",  # Replace with actual path
        entrypoint="data_processing_flow",
    ).deploy(
        name="data_processing_pipeline",
        work_pool_name="my-managed-pool",  # Replace with your Prefect Cloud work pool name
        # Schedule can be set here too (optional)
    )
