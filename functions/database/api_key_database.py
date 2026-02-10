from __future__ import annotations

import hashlib
import secrets
from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from neo4j import Session

    from .database import Database


class ApiKeyDatabase:
    CREATE_QUERY = """
        CREATE (k:ApiKey {
            id: $key_id,
            name: $name,
            email: $email,
            api_key_hash: $api_key_hash,
            is_active: true,
            created_at: datetime($created_at)
        })
        RETURN k.id AS id
    """

    CREATE_WITH_BINDING_QUERY = """
        MATCH (a:Application {id: $application_id})
        CREATE (k:ApiKey {
            id: $key_id,
            name: $name,
            email: $email,
            api_key_hash: $api_key_hash,
            is_active: true,
            created_at: datetime($created_at)
        })-[:BOUND_TO]->(a)
        RETURN k.id AS id
    """

    VALIDATE_KEY_QUERY = """
        MATCH (k:ApiKey {api_key_hash: $api_key_hash, is_active: true})
        OPTIONAL MATCH (k)-[:BOUND_TO]->(a:Application)
        RETURN k.id AS id, a.id AS bound_application_id
    """

    REVOKE_QUERY = """
        MATCH (k:ApiKey {id: $key_id})
        SET k.is_active = false
        RETURN k.id AS id
    """

    ROTATE_KEY_QUERY = """
        MATCH (k:ApiKey {id: $key_id})
        SET k.api_key_hash = $api_key_hash, k.is_active = true
        RETURN k.id AS id
    """

    LIST_QUERY = """
        MATCH (k:ApiKey)
        OPTIONAL MATCH (k)-[:BOUND_TO]->(a:Application)
        RETURN k.id AS id, k.name AS name, k.email AS email, k.is_active AS is_active,
               k.created_at AS created_at, a.id AS bound_application_id
        ORDER BY k.created_at DESC
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    @property
    def session(self) -> Session:
        return self._db.get_session()

    @staticmethod
    def _hash_key(raw_key: str) -> str:
        """Hash an API key using SHA-256."""
        return hashlib.sha256(raw_key.encode()).hexdigest()

    @staticmethod
    def _generate_api_key() -> str:
        """Generate a secure random API key (32 characters)."""
        return secrets.token_urlsafe(24)

    def create(self, key_id: str, name: str, email: str, application_id: str | None = None) -> tuple[str, str]:
        """
        Create a new API key, optionally bound to an application.

        Args:
            key_id: Unique identifier for the API key.
            name: Human-readable name for the key.
            email: Contact email for the key owner.
            application_id: Optional application ID to bind the key to.

        Returns:
            Tuple of (key_id, raw_api_key).
            The raw API key is only returned once and should be saved by the caller.
        """
        raw_key = self._generate_api_key()
        api_key_hash = self._hash_key(raw_key)
        created_at = datetime.now(UTC).isoformat()

        with self.session as session:
            if application_id:
                result = session.run(
                    self.CREATE_WITH_BINDING_QUERY,
                    key_id=key_id,
                    name=name,
                    email=email,
                    api_key_hash=api_key_hash,
                    created_at=created_at,
                    application_id=application_id,
                ).single()
            else:
                result = session.run(
                    self.CREATE_QUERY,
                    key_id=key_id,
                    name=name,
                    email=email,
                    api_key_hash=api_key_hash,
                    created_at=created_at,
                ).single()

            if result is None:
                if application_id:
                    raise ValueError(f"Application '{application_id}' not found")
                raise ValueError("Failed to create API key")

            return result["id"], raw_key

    def validate_key(self, raw_key: str) -> dict | None:
        """
        Validate an API key and return key info.

        Returns:
            Dict with 'id' and 'bound_application_id' (None if not bound),
            or None if the key is invalid or inactive.
        """
        api_key_hash = self._hash_key(raw_key)

        with self.session as session:
            result = session.run(
                self.VALIDATE_KEY_QUERY,
                api_key_hash=api_key_hash,
            ).single()
            if result is None:
                return None
            return {
                "id": result["id"],
                "bound_application_id": result["bound_application_id"],
            }

    def revoke(self, key_id: str) -> bool:
        """
        Revoke an API key by setting is_active to false.

        Returns:
            True if the key was found and revoked, False otherwise.
        """
        with self.session as session:
            result = session.run(self.REVOKE_QUERY, key_id=key_id).single()
            return result is not None

    def rotate_key(self, key_id: str) -> str | None:
        """
        Generate a new API key value for an existing key.

        Returns:
            The new raw API key, or None if the key was not found.
        """
        raw_key = self._generate_api_key()
        api_key_hash = self._hash_key(raw_key)

        with self.session as session:
            result = session.run(
                self.ROTATE_KEY_QUERY,
                key_id=key_id,
                api_key_hash=api_key_hash,
            ).single()

            if result is None:
                return None
            return raw_key

    def list_all(self) -> list[dict]:
        """
        List all API keys.

        Returns:
            List of key dictionaries with id, name, email, is_active, created_at, bound_application_id.
        """
        with self.session as session:
            return session.run(self.LIST_QUERY).data()
