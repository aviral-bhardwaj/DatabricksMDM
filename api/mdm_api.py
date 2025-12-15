# api/mdm_api.py

from fastapi import FastAPI, HTTPException, Depends
from databricks.sdk import WorkspaceClient
from pydantic import BaseModel
import os

app = FastAPI(title="MDM Product API")

# Initialize Databricks client
w = WorkspaceClient(
    host=os.getenv("DATABRICKS_HOST"),
    token=os.getenv("DATABRICKS_TOKEN")
)


class EntitySearchRequest(BaseModel):
    entity_type: str
    search_criteria: dict
    match_threshold: float = 0.85


class EntityCreateRequest(BaseModel):
    entity_type: str
    source_system: str
    entity_data: dict


@app.post("/api/v1/entities/search")
async def search_entities(request: EntitySearchRequest):
    """
    Search for master entities
    """
    # Trigger Databricks job for search
    job_run = w.jobs.run_now(
        job_id=int(os.getenv("SEARCH_JOB_ID")),
        notebook_params={
            "entity_type": request.entity_type,
            "search_criteria": json.dumps(request.search_criteria),
            "threshold": str(request.match_threshold)
        }
    )

    # Wait for completion and get results
    run_state = w.jobs.wait_get_run_job_terminated_or_skipped(job_run.run_id)

    if run_state.state.result_state == "SUCCESS":
        # Read results from Delta table
        results = _read_search_results(job_run.run_id)
        return {"status": "success", "data": results}
    else:
        raise HTTPException(status_code=500, detail="Search failed")


@app.post("/api/v1/entities")
async def create_entity(request: EntityCreateRequest):
    """
    Create new master entity
    """
    # Trigger ingestion and matching workflow
    job_run = w.jobs.run_now(
        job_id=int(os.getenv("INGESTION_JOB_ID")),
        notebook_params={
            "entity_type": request.entity_type,
            "source_system": request.source_system,
            "entity_data": json.dumps(request.entity_data)
        }
    )

    return {"status": "processing", "run_id": job_run.run_id}


@app.get("/api/v1/entities/{master_id}")
async def get_entity(master_id: str):
    """
    Get golden record by master ID
    """
    # Query Delta table directly
    golden_record = _query_golden_record(master_id)

    if golden_record:
        return {"status": "success", "data": golden_record}
    else:
        raise HTTPException(status_code=404, detail="Entity not found")


@app.get("/api/v1/lineage/{master_id}")
async def get_lineage(master_id: str):
    """
    Get data lineage for master entity
    """
    lineage = _get_entity_lineage(master_id)
    return {"status": "success", "data": lineage}