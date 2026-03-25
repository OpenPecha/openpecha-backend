# openpecha-backend

FastAPI-based backend for OpenPecha.

## Project Structure

```
openpecha-backend/
├── main.py                 # FastAPI entry point
├── routers/                # API route handlers
├── database/               # Database layer
├── models.py               # Pydantic models
├── tests/                  # Test suite
├── requirements.txt        # Production dependencies
├── requirements_dev.txt    # Development dependencies
└── gunicorn.conf.py        # Production server config
```

## Setup

1. Clone the repo

2. Create a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install dependencies

```bash
pip install -r requirements.txt
pip install -r requirements_dev.txt  # For development
```

4. Copy environment template and configure

```bash
cp .env.example .env
# Edit .env with your Neo4j and AWS credentials
```

## Running the API

### Development

```bash
uvicorn main:app --reload
```

The API will be available at `http://localhost:8000`

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

### Production

```bash
gunicorn main:app -c gunicorn.conf.py
```

## Running Tests

Tests spin up a disposable Neo4j container automatically via
[testcontainers](https://testcontainers-python.readthedocs.io/). No manual Neo4j
installation or env vars are needed.

### Prerequisites

1. Install dev dependencies:
   ```bash
   pip install -r requirements_dev.txt
   ```

2. You need a running **Docker-compatible runtime** (Docker Desktop, Colima,
   Podman, etc.)

   - **Docker Desktop** works out of the box
   - **Other runtimes** — set `DOCKER_HOST`:
     ```bash
     export DOCKER_HOST=unix://$HOME/.colima/default/docker.sock
     ```

### Run tests

```bash
pytest tests/
```

Or with verbose output:

```bash
python -m pytest tests/ -v
```

The first run will pull the `neo4j:2025` image (~500 MB), which is cached for
subsequent runs.

## Deployment

Deployment is handled automatically via GitHub Actions when pushing to `main`.

### Manual Deployment (EC2)

```bash
ssh ubuntu@<EC2_HOST>
cd /opt/openpecha-backend
git pull origin main
source .venv/bin/activate
pip install -r requirements.txt
sudo systemctl restart openpecha-api
```

### Environment Configuration

- **Neo4j**: Configured via `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD` in
  `.env`
- **AWS S3**: Configured via `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
  `AWS_S3_BUCKET` in `.env`

## Documentation

API documentation available at: https://pecha-backend.web.app/docs
