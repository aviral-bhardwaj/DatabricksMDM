# workflows/mdm_pipeline.py

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs


def create_mdm_workflow():
    """
    Create end-to-end MDM workflow
    """
    w = WorkspaceClient()

    mdm_workflow = w.jobs.create(
        name="MDM_End_to_End_Pipeline",
        tasks=[
            # Task 1: Ingest from all sources
            jobs.Task(
                task_key="ingest_sources",
                notebook_task=jobs.NotebookTask(
                    notebook_path="/Workspace/mdm/01_ingestion/run_ingestion",
                    base_parameters={"entity_type": "customer"}
                ),
                new_cluster=jobs.ClusterSpec(
                    spark_version="14.3.x-scala2.12",
                    node_type_id="i3.xlarge",
                    num_workers=2
                )
            ),

            # Task 2: Entity matching
            jobs.Task(
                task_key="entity_matching",
                depends_on=[jobs.TaskDependency(task_key="ingest_sources")],
                notebook_task=jobs.NotebookTask(
                    notebook_path="/Workspace/mdm/02_matching/run_matching",
                    base_parameters={"entity_type": "customer"}
                ),
                new_cluster=jobs.ClusterSpec(
                    spark_version="14.3.x-scala2.12",
                    node_type_id="i3.2xlarge",
                    num_workers=4
                )
            ),

            # Task 3: Golden record creation
            jobs.Task(
                task_key="golden_records",
                depends_on=[jobs.TaskDependency(task_key="entity_matching")],
                notebook_task=jobs.NotebookTask(
                    notebook_path="/Workspace/mdm/03_golden_record/build_golden",
                    base_parameters={"entity_type": "customer"}
                ),
                new_cluster=jobs.ClusterSpec(
                    spark_version="14.3.x-scala2.12",
                    node_type_id="i3.xlarge",
                    num_workers=2
                )
            ),

            # Task 4: Data quality checks
            jobs.Task(
                task_key="quality_checks",
                depends_on=[jobs.TaskDependency(task_key="golden_records")],
                notebook_task=jobs.NotebookTask(
                    notebook_path="/Workspace/mdm/04_quality/run_dq",
                    base_parameters={"entity_type": "customer"}
                ),
                new_cluster=jobs.ClusterSpec(
                    spark_version="14.3.x-scala2.12",
                    node_type_id="i3.xlarge",
                    num_workers=1
                )
            ),

            # Task 5: Publish to downstream
            jobs.Task(
                task_key="publish_downstream",
                depends_on=[jobs.TaskDependency(task_key="quality_checks")],
                notebook_task=jobs.NotebookTask(
                    notebook_path="/Workspace/mdm/05_publish/publish_golden",
                    base_parameters={"entity_type": "customer"}
                ),
                new_cluster=jobs.ClusterSpec(
                    spark_version="14.3.x-scala2.12",
                    node_type_id="i3.xlarge",
                    num_workers=1
                )
            )
        ],
        schedule=jobs.CronSchedule(
            quartz_cron_expression="0 0 2 * * ?",  # Daily at 2 AM
            timezone_id="America/Los_Angeles"
        )
    )

    return mdm_workflow