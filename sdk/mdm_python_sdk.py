"""
Databricks MDM Python SDK
Enterprise Master Data Management SDK
"""

import requests
from typing import List, Dict, Optional
import json
from datetime import datetime


class MDMClient:
    """
    Python SDK for Databricks MDM API
    """

    def __init__(self, api_url: str, api_key: str):
        """
        Initialize MDM Client

        Args:
            api_url: Base URL of MDM API
            api_key: API authentication key
        """
        self.api_url = api_url.rstrip('/')
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        })

    # Entity Management

    def search_entities(self, entity_type: str, search_criteria: dict,
                       match_threshold: float = 0.85, limit: int = 100) -> Dict:
        """
        Search for master entities

        Args:
            entity_type: Type of entity (customer, product, etc.)
            search_criteria: Search parameters
            match_threshold: Matching threshold (0-1)
            limit: Maximum number of results

        Returns:
            Search results
        """
        payload = {
            "entity_type": entity_type,
            "search_criteria": search_criteria,
            "match_threshold": match_threshold,
            "limit": limit
        }

        response = self.session.post(
            f"{self.api_url}/api/v1/entities/search",
            json=payload
        )

        response.raise_for_status()
        return response.json()

    def create_entity(self, entity_type: str, source_system: str,
                     entity_data: dict) -> Dict:
        """
        Create a new master entity

        Args:
            entity_type: Type of entity
            source_system: Source system name
            entity_data: Entity data dictionary

        Returns:
            Creation status and run ID
        """
        payload = {
            "entity_type": entity_type,
            "source_system": source_system,
            "entity_data": entity_data
        }

        response = self.session.post(
            f"{self.api_url}/api/v1/entities",
            json=payload
        )

        response.raise_for_status()
        return response.json()

    def get_entity(self, master_id: str) -> Dict:
        """
        Get golden record by master ID

        Args:
            master_id: Master entity ID

        Returns:
            Golden record data
        """
        response = self.session.get(
            f"{self.api_url}/api/v1/entities/{master_id}"
        )

        response.raise_for_status()
        return response.json()

    def update_entity(self, master_id: str, field_updates: dict,
                     update_reason: Optional[str] = None) -> Dict:
        """
        Update golden record

        Args:
            master_id: Master entity ID
            field_updates: Fields to update
            update_reason: Reason for update

        Returns:
            Update status
        """
        payload = {
            "field_updates": field_updates,
            "update_reason": update_reason
        }

        response = self.session.put(
            f"{self.api_url}/api/v1/entities/{master_id}",
            json=payload
        )

        response.raise_for_status()
        return response.json()

    def delete_entity(self, master_id: str) -> Dict:
        """
        Soft delete golden record

        Args:
            master_id: Master entity ID

        Returns:
            Deletion status
        """
        response = self.session.delete(
            f"{self.api_url}/api/v1/entities/{master_id}"
        )

        response.raise_for_status()
        return response.json()

    # Manual Overrides

    def create_override(self, master_id: str, field_name: str,
                       override_value: any, reason: str) -> Dict:
        """
        Create manual override for a field

        Args:
            master_id: Master entity ID
            field_name: Field to override
            override_value: New value
            reason: Reason for override

        Returns:
            Override creation status
        """
        payload = {
            "field_name": field_name,
            "override_value": override_value,
            "reason": reason
        }

        response = self.session.post(
            f"{self.api_url}/api/v1/entities/{master_id}/overrides",
            json=payload
        )

        response.raise_for_status()
        return response.json()

    def remove_override(self, master_id: str, field_name: str) -> Dict:
        """
        Remove manual override

        Args:
            master_id: Master entity ID
            field_name: Field to remove override from

        Returns:
            Removal status
        """
        response = self.session.delete(
            f"{self.api_url}/api/v1/entities/{master_id}/overrides/{field_name}"
        )

        response.raise_for_status()
        return response.json()

    # Match Review

    def get_pending_matches(self, limit: int = 100) -> Dict:
        """
        Get pending match reviews

        Args:
            limit: Maximum number of matches

        Returns:
            Pending matches
        """
        response = self.session.get(
            f"{self.api_url}/api/v1/matches/pending",
            params={"limit": limit}
        )

        response.raise_for_status()
        return response.json()

    def approve_match(self, review_id: str, notes: Optional[str] = None) -> Dict:
        """
        Approve a match

        Args:
            review_id: Review ID
            notes: Optional notes

        Returns:
            Approval status
        """
        payload = {
            "action": "approve",
            "notes": notes
        }

        response = self.session.post(
            f"{self.api_url}/api/v1/matches/{review_id}/review",
            json=payload
        )

        response.raise_for_status()
        return response.json()

    def reject_match(self, review_id: str, notes: Optional[str] = None) -> Dict:
        """
        Reject a match

        Args:
            review_id: Review ID
            notes: Optional notes

        Returns:
            Rejection status
        """
        payload = {
            "action": "reject",
            "notes": notes
        }

        response = self.session.post(
            f"{self.api_url}/api/v1/matches/{review_id}/review",
            json=payload
        )

        response.raise_for_status()
        return response.json()

    # Data Quality

    def get_quality_scores(self, entity_type: Optional[str] = None) -> Dict:
        """
        Get data quality scores

        Args:
            entity_type: Optional entity type filter

        Returns:
            Quality scores
        """
        params = {"entity_type": entity_type} if entity_type else {}

        response = self.session.get(
            f"{self.api_url}/api/v1/quality/scores",
            params=params
        )

        response.raise_for_status()
        return response.json()

    def get_quality_issues(self, entity_type: str,
                          severity_filter: Optional[str] = None) -> Dict:
        """
        Get data quality issues

        Args:
            entity_type: Entity type
            severity_filter: Filter by severity (CRITICAL, HIGH, MEDIUM)

        Returns:
            Quality issues
        """
        payload = {
            "entity_type": entity_type,
            "severity_filter": severity_filter
        }

        response = self.session.get(
            f"{self.api_url}/api/v1/quality/issues",
            json=payload
        )

        response.raise_for_status()
        return response.json()

    def assign_quality_issue(self, issue_id: str, assigned_to: str) -> Dict:
        """
        Assign quality issue for remediation

        Args:
            issue_id: Issue ID
            assigned_to: Email of assignee

        Returns:
            Assignment status
        """
        response = self.session.post(
            f"{self.api_url}/api/v1/quality/issues/{issue_id}/assign",
            params={"assigned_to": assigned_to}
        )

        response.raise_for_status()
        return response.json()

    # Lineage

    def get_table_lineage(self, table_name: str, direction: str = "both",
                         depth: int = 5) -> Dict:
        """
        Get data lineage for a table

        Args:
            table_name: Table name
            direction: "upstream", "downstream", or "both"
            depth: Traversal depth

        Returns:
            Lineage information
        """
        params = {
            "direction": direction,
            "depth": depth
        }

        response = self.session.get(
            f"{self.api_url}/api/v1/lineage/table/{table_name}",
            params=params
        )

        response.raise_for_status()
        return response.json()

    def get_entity_lineage(self, master_id: str) -> Dict:
        """
        Get data lineage for an entity

        Args:
            master_id: Master entity ID

        Returns:
            Entity lineage
        """
        response = self.session.get(
            f"{self.api_url}/api/v1/lineage/entity/{master_id}"
        )

        response.raise_for_status()
        return response.json()

    # Audit

    def get_entity_audit_history(self, entity_id: str, limit: int = 100) -> Dict:
        """
        Get audit history for an entity

        Args:
            entity_id: Entity ID
            limit: Maximum number of records

        Returns:
            Audit history
        """
        params = {"limit": limit}

        response = self.session.get(
            f"{self.api_url}/api/v1/audit/entity/{entity_id}",
            params=params
        )

        response.raise_for_status()
        return response.json()

    def get_user_activity(self, user_email: str, days: int = 30) -> Dict:
        """
        Get user activity

        Args:
            user_email: User email
            days: Number of days to look back

        Returns:
            User activity
        """
        params = {"days": days}

        response = self.session.get(
            f"{self.api_url}/api/v1/audit/user/{user_email}",
            params=params
        )

        response.raise_for_status()
        return response.json()

    # Webhooks

    def subscribe_webhook(self, webhook_url: str, events: List[str],
                         secret: Optional[str] = None) -> Dict:
        """
        Subscribe to webhook events

        Args:
            webhook_url: Webhook URL
            events: List of events to subscribe to
            secret: Optional webhook secret

        Returns:
            Subscription ID
        """
        payload = {
            "webhook_url": webhook_url,
            "events": events,
            "secret": secret
        }

        response = self.session.post(
            f"{self.api_url}/api/v1/webhooks/subscribe",
            json=payload
        )

        response.raise_for_status()
        return response.json()

    def unsubscribe_webhook(self, subscription_id: str) -> Dict:
        """
        Unsubscribe from webhook

        Args:
            subscription_id: Subscription ID

        Returns:
            Unsubscribe status
        """
        response = self.session.delete(
            f"{self.api_url}/api/v1/webhooks/{subscription_id}"
        )

        response.raise_for_status()
        return response.json()

    def list_webhooks(self) -> Dict:
        """
        List all webhook subscriptions

        Returns:
            List of subscriptions
        """
        response = self.session.get(
            f"{self.api_url}/api/v1/webhooks"
        )

        response.raise_for_status()
        return response.json()

    # Health Check

    def health_check(self) -> Dict:
        """
        Check API health

        Returns:
            Health status
        """
        response = self.session.get(f"{self.api_url}/health")
        response.raise_for_status()
        return response.json()


# Example usage
if __name__ == "__main__":
    # Initialize client
    client = MDMClient(
        api_url="https://your-mdm-api.com",
        api_key="your-api-key"
    )

    # Search for customers
    results = client.search_entities(
        entity_type="customer",
        search_criteria={"email": "john.doe@example.com"}
    )

    print(f"Found {results['count']} matching entities")

    # Create new entity
    new_entity = client.create_entity(
        entity_type="customer",
        source_system="Salesforce",
        entity_data={
            "name": "John Doe",
            "email": "john.doe@example.com",
            "phone": "+1-555-1234"
        }
    )

    print(f"Created entity with run_id: {new_entity['run_id']}")

    # Subscribe to webhooks
    subscription = client.subscribe_webhook(
        webhook_url="https://your-app.com/webhooks/mdm",
        events=["entity.created", "entity.updated", "match.requires_review"]
    )

    print(f"Webhook subscription created: {subscription['subscription_id']}")
