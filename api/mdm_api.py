# api/mdm_api.py

from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks, Header
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from databricks.sdk import WorkspaceClient
from pydantic import BaseModel, EmailStr
from typing import Optional, List, Dict
import os
import json
from datetime import datetime
import asyncio
import aiohttp

app = FastAPI(
    title="MDM Product API",
    version="1.0.0",
    description="Enterprise Master Data Management API"
)

# Security
security = HTTPBearer()

# Initialize Databricks client
w = WorkspaceClient(
    host=os.getenv("DATABRICKS_HOST"),
    token=os.getenv("DATABRICKS_TOKEN")
)

# Webhook manager
webhook_manager = None  # Will be initialized


# Request/Response Models
class EntitySearchRequest(BaseModel):
    entity_type: str
    search_criteria: dict
    match_threshold: float = 0.85
    limit: int = 100


class EntityCreateRequest(BaseModel):
    entity_type: str
    source_system: str
    entity_data: dict


class EntityUpdateRequest(BaseModel):
    field_updates: Dict[str, any]
    update_reason: Optional[str] = None


class ManualOverrideRequest(BaseModel):
    field_name: str
    override_value: any
    reason: str


class MatchReviewRequest(BaseModel):
    action: str  # "approve" or "reject"
    notes: Optional[str] = None


class WebhookSubscription(BaseModel):
    webhook_url: str
    events: List[str]
    secret: Optional[str] = None


class DataQualityRequest(BaseModel):
    entity_type: str
    severity_filter: Optional[str] = None


# Entity Management Endpoints

@app.post("/api/v1/entities/search")
async def search_entities(request: EntitySearchRequest, credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Search for master entities"""
    job_run = w.jobs.run_now(
        job_id=int(os.getenv("SEARCH_JOB_ID")),
        notebook_params={
            "entity_type": request.entity_type,
            "search_criteria": json.dumps(request.search_criteria),
            "threshold": str(request.match_threshold),
            "limit": str(request.limit)
        }
    )

    run_state = w.jobs.wait_get_run_job_terminated_or_skipped(job_run.run_id)

    if run_state.state.result_state == "SUCCESS":
        results = _read_search_results(job_run.run_id)
        return {"status": "success", "data": results, "count": len(results)}
    else:
        raise HTTPException(status_code=500, detail="Search failed")


@app.post("/api/v1/entities", status_code=201)
async def create_entity(request: EntityCreateRequest, background_tasks: BackgroundTasks,
                       credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Create new master entity"""
    job_run = w.jobs.run_now(
        job_id=int(os.getenv("INGESTION_JOB_ID")),
        notebook_params={
            "entity_type": request.entity_type,
            "source_system": request.source_system,
            "entity_data": json.dumps(request.entity_data)
        }
    )

    # Trigger webhook in background
    background_tasks.add_task(
        trigger_webhook,
        "entity.created",
        {"entity_type": request.entity_type, "source_system": request.source_system}
    )

    return {"status": "processing", "run_id": job_run.run_id}


@app.get("/api/v1/entities/{master_id}")
async def get_entity(master_id: str, credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Get golden record by master ID"""
    golden_record = _query_golden_record(master_id)

    if golden_record:
        return {"status": "success", "data": golden_record}
    else:
        raise HTTPException(status_code=404, detail="Entity not found")


@app.put("/api/v1/entities/{master_id}")
async def update_entity(master_id: str, request: EntityUpdateRequest, background_tasks: BackgroundTasks,
                       credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Update golden record"""
    success = _update_golden_record(master_id, request.field_updates, request.update_reason)

    if success:
        background_tasks.add_task(trigger_webhook, "entity.updated", {"master_id": master_id})
        return {"status": "success", "master_id": master_id}
    else:
        raise HTTPException(status_code=404, detail="Entity not found")


@app.delete("/api/v1/entities/{master_id}")
async def delete_entity(master_id: str, background_tasks: BackgroundTasks,
                       credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Soft delete golden record"""
    success = _soft_delete_entity(master_id)

    if success:
        background_tasks.add_task(trigger_webhook, "entity.deleted", {"master_id": master_id})
        return {"status": "success", "message": "Entity soft deleted"}
    else:
        raise HTTPException(status_code=404, detail="Entity not found")


# Manual Override Endpoints

@app.post("/api/v1/entities/{master_id}/overrides")
async def create_override(master_id: str, request: ManualOverrideRequest,
                         credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Create manual override for a field"""
    user = _get_user_from_token(credentials.credentials)

    success = _create_manual_override(
        master_id,
        request.field_name,
        request.override_value,
        user,
        request.reason
    )

    if success:
        return {"status": "success", "message": "Override created"}
    else:
        raise HTTPException(status_code=400, detail="Failed to create override")


@app.delete("/api/v1/entities/{master_id}/overrides/{field_name}")
async def remove_override(master_id: str, field_name: str,
                         credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Remove manual override"""
    user = _get_user_from_token(credentials.credentials)

    success = _remove_manual_override(master_id, field_name, user)

    if success:
        return {"status": "success", "message": "Override removed"}
    else:
        raise HTTPException(status_code=404, detail="Override not found")


# Match Review Endpoints

@app.get("/api/v1/matches/pending")
async def get_pending_matches(limit: int = 100, credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Get pending match reviews"""
    matches = _get_pending_reviews(limit)
    return {"status": "success", "data": matches, "count": len(matches)}


@app.post("/api/v1/matches/{review_id}/review")
async def review_match(review_id: str, request: MatchReviewRequest, background_tasks: BackgroundTasks,
                      credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Approve or reject a match"""
    user = _get_user_from_token(credentials.credentials)

    if request.action == "approve":
        success = _approve_match(review_id, user, request.notes)
        event = "match.approved"
    elif request.action == "reject":
        success = _reject_match(review_id, user, request.notes)
        event = "match.rejected"
    else:
        raise HTTPException(status_code=400, detail="Invalid action")

    if success:
        background_tasks.add_task(trigger_webhook, event, {"review_id": review_id})
        return {"status": "success", "action": request.action}
    else:
        raise HTTPException(status_code=404, detail="Review not found")


# Data Quality Endpoints

@app.get("/api/v1/quality/scores")
async def get_quality_scores(entity_type: Optional[str] = None,
                            credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Get data quality scores"""
    scores = _get_quality_scores(entity_type)
    return {"status": "success", "data": scores}


@app.get("/api/v1/quality/issues")
async def get_quality_issues(request: DataQualityRequest,
                            credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Get data quality issues"""
    issues = _get_quality_issues(request.entity_type, request.severity_filter)
    return {"status": "success", "data": issues, "count": len(issues)}


@app.post("/api/v1/quality/issues/{issue_id}/assign")
async def assign_quality_issue(issue_id: str, assigned_to: EmailStr,
                              credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Assign quality issue for remediation"""
    success = _assign_quality_issue(issue_id, assigned_to)

    if success:
        return {"status": "success", "message": f"Issue assigned to {assigned_to}"}
    else:
        raise HTTPException(status_code=404, detail="Issue not found")


# Lineage Endpoints

@app.get("/api/v1/lineage/table/{table_name}")
async def get_table_lineage(table_name: str, direction: str = "both", depth: int = 5,
                           credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Get data lineage for a table"""
    lineage = _get_table_lineage(table_name, direction, depth)
    return {"status": "success", "data": lineage}


@app.get("/api/v1/lineage/entity/{master_id}")
async def get_entity_lineage(master_id: str, credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Get data lineage for an entity"""
    lineage = _get_entity_lineage(master_id)
    return {"status": "success", "data": lineage}


# Audit Endpoints

@app.get("/api/v1/audit/entity/{entity_id}")
async def get_entity_audit_history(entity_id: str, limit: int = 100,
                                   credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Get audit history for an entity"""
    history = _get_audit_history(entity_id, limit)
    return {"status": "success", "data": history}


@app.get("/api/v1/audit/user/{user_email}")
async def get_user_activity(user_email: EmailStr, days: int = 30,
                           credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Get user activity"""
    activity = _get_user_activity(user_email, days)
    return {"status": "success", "data": activity}


# Webhook Endpoints

@app.post("/api/v1/webhooks/subscribe")
async def subscribe_webhook(request: WebhookSubscription,
                           credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Subscribe to webhook events"""
    subscription_id = _create_webhook_subscription(
        request.webhook_url,
        request.events,
        request.secret
    )

    return {"status": "success", "subscription_id": subscription_id}


@app.delete("/api/v1/webhooks/{subscription_id}")
async def unsubscribe_webhook(subscription_id: str,
                              credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Unsubscribe from webhook"""
    success = _delete_webhook_subscription(subscription_id)

    if success:
        return {"status": "success", "message": "Subscription deleted"}
    else:
        raise HTTPException(status_code=404, detail="Subscription not found")


@app.get("/api/v1/webhooks")
async def list_webhooks(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """List all webhook subscriptions"""
    subscriptions = _list_webhook_subscriptions()
    return {"status": "success", "data": subscriptions}


# Health Check

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


# Helper Functions (implementations would connect to Databricks)

def _query_golden_record(master_id: str):
    # Implementation
    pass


def _read_search_results(run_id: str):
    # Implementation
    pass


def _update_golden_record(master_id: str, updates: dict, reason: str):
    # Implementation
    pass


def _soft_delete_entity(master_id: str):
    # Implementation
    pass


def _create_manual_override(master_id: str, field: str, value: any, user: str, reason: str):
    # Implementation
    pass


def _remove_manual_override(master_id: str, field: str, user: str):
    # Implementation
    pass


def _get_pending_reviews(limit: int):
    # Implementation
    pass


def _approve_match(review_id: str, user: str, notes: str):
    # Implementation
    pass


def _reject_match(review_id: str, user: str, notes: str):
    # Implementation
    pass


def _get_quality_scores(entity_type: str):
    # Implementation
    pass


def _get_quality_issues(entity_type: str, severity: str):
    # Implementation
    pass


def _assign_quality_issue(issue_id: str, assigned_to: str):
    # Implementation
    pass


def _get_table_lineage(table_name: str, direction: str, depth: int):
    # Implementation
    pass


def _get_entity_lineage(master_id: str):
    # Implementation
    pass


def _get_audit_history(entity_id: str, limit: int):
    # Implementation
    pass


def _get_user_activity(user_email: str, days: int):
    # Implementation
    pass


def _create_webhook_subscription(url: str, events: List[str], secret: str):
    # Implementation
    pass


def _delete_webhook_subscription(subscription_id: str):
    # Implementation
    pass


def _list_webhook_subscriptions():
    # Implementation
    pass


def _get_user_from_token(token: str):
    # Implementation
    return "user@example.com"


async def trigger_webhook(event_type: str, data: dict):
    """Trigger webhook notifications"""
    subscriptions = _list_webhook_subscriptions()

    async with aiohttp.ClientSession() as session:
        for sub in subscriptions:
            if event_type in sub['events']:
                payload = {
                    "event": event_type,
                    "timestamp": datetime.now().isoformat(),
                    "data": data
                }

                try:
                    async with session.post(sub['webhook_url'], json=payload, timeout=10) as response:
                        if response.status != 200:
                            print(f"Webhook delivery failed: {sub['webhook_url']}")
                except Exception as e:
                    print(f"Webhook error: {e}")