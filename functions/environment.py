import os
import logging

logger = logging.getLogger(__name__)


def get_environment() -> str:
    """
    Detect current environment based on:
    1. GCP_PROJECT (set automatically in deployed Cloud Functions)
    2. GCLOUD_PROJECT (set automatically in deployed Cloud Functions)
    3. ENVIRONMENT variable (for local emulator)
    
    Returns: 'dev', 'test', or 'prod'
    """
    # Check if running in deployed Cloud Function
    project_id = os.environ.get('GCP_PROJECT') or os.environ.get('GCLOUD_PROJECT')
    
    if project_id:
        if project_id == 'pecha-backend-test-3a4d0':
            logger.info(f"Detected environment: test (project: {project_id})")
            return 'test'
        elif project_id == 'pecha-backend':
            logger.info(f"Detected environment: prod (project: {project_id})")
            return 'prod'
        elif project_id == 'pecha-backend-dev':
            logger.info(f"Detected environment: dev (project: {project_id})")
            return 'dev'
    
    # Fallback to ENVIRONMENT variable (for local emulator)
    env = os.environ.get('ENVIRONMENT', 'dev').lower()
    logger.info(f"Using environment from ENVIRONMENT variable: {env}")
    return env


def get_neo4j_credentials() -> dict:
    """
    Get Neo4j credentials for the current environment.
    
    Returns dict with keys: uri, password, username, database
    """
    env = get_environment()
    env_upper = env.upper()
    
    # For deployed environments, use Firebase secrets
    # Firebase secrets are named NEO4J_URI, NEO4J_PASSWORD (without prefix)
    if os.environ.get('K_SERVICE'):  # K_SERVICE is set in Cloud Run
        credentials = {
            'uri': os.environ.get('NEO4J_URI'),
            'password': os.environ.get('NEO4J_PASSWORD'),
            'username': os.environ.get('NEO4J_USERNAME', 'neo4j'),
            'database': os.environ.get('NEO4J_DATABASE', 'neo4j')
        }
        logger.info(f"Using Firebase secrets for {env} environment")
    else:
        # For local emulator, use environment-prefixed variables from .env
        credentials = {
            'uri': os.environ.get(f'NEO4J_{env_upper}_URI'),
            'password': os.environ.get(f'NEO4J_{env_upper}_PASSWORD'),
            'username': os.environ.get(f'NEO4J_{env_upper}_USERNAME', 'neo4j'),
            'database': os.environ.get(f'NEO4J_{env_upper}_DATABASE', 'neo4j')
        }
        logger.info(f"Using .env credentials for {env} environment")
    
    if not credentials['uri'] or not credentials['password']:
        raise ValueError(f"Missing Neo4j credentials for environment: {env}")
    
    # Log credentials (mask password for security)
    masked_password = credentials['password'][:4] + "****" if credentials['password'] else "None"
    logger.info(f"Neo4j credentials: URI={credentials['uri']}, Password={masked_password}, Username={credentials['username']}")
    
    return credentials

