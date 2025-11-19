# openpecha-backend

## Setup instructions
1. Clone the repo
2. Install Firebase CLI
```
npm install -g firebase-tools
```
3. Login to Firebase
```
firebase login
```
4. Login to Google Cloud
```
gcloud auth application-default login
```

## Preparing to build the backend
1. Navigate to the `functions` directory
2. Create a virtual environment
```
python -m venv venv
source venv/bin/activate  # On Windows use: venv\Scripts\activate
```
3. Install required dependencies
```
pip install -r requirements.txt
```

## Testing the backend locally
Run the emulator to test Cloud Functions locally:
```
firebase emulators:start --only functions
```
The functions will be available at:
```
"http://127.0.0.1:5001/pecha-backend/us-central1/api/{function-name}
```

## Testing the frontend locally
Run the local hosting emulator:
```
firebase emulators:start --only hosting
```
The site will be available at:
```
http://localhost:5000
```

## Deploying backend
Once your functions are working locally, deploy them to Firebase:
### Dev (default)
```
firebase deploy --only functions
```
### Test
```
# Switch to test branch first
git checkout test

# Deploy to test project
firebase deploy --only functions --project test
```
### Production
```
firebase deploy --only functions --project prod
```

## Deploying frontend
When you’re ready to publish changes:
### Dev (default)
```
firebase deploy --only hosting
```
The website will be available at:
```
https://pecha-backend-dev.web.app
```
### Production
```
firebase deploy --only hosting --project prod
```
The website will be available at:
```
https://pecha-backend.web.app
```

## Testing Different Environments Locally

To test against different Neo4j databases locally, edit `functions/.env` and change the `ENVIRONMENT` variable:

```bash
# Test against dev database
ENVIRONMENT=dev

# Test against test database  
ENVIRONMENT=test

# Test against prod database
ENVIRONMENT=prod
```

Then restart the emulator:
```bash
firebase emulators:start --only functions
```

**Note:** Deployed environments automatically detect which database to use based on the Firebase project ID. The `.env` file is only used for local development.

### Environment Variables in `.env`

Your `functions/.env` file should contain credentials for all environments:

```bash
# Local environment selector
ENVIRONMENT=dev

# Development Neo4j Database
NEO4J_DEV_URI=neo4j+s://0b1a8ea7.databases.neo4j.io
NEO4J_DEV_PASSWORD=your-dev-password
NEO4J_DEV_USERNAME=neo4j
NEO4J_DEV_DATABASE=neo4j

# Test Neo4j Database
NEO4J_TEST_URI=neo4j+s://7d1b2e55.databases.neo4j.io
NEO4J_TEST_PASSWORD=s3MdyYbT61Ho17EWCsdi2_YOsMvCLudCOg4xvt5IJ-o
NEO4J_TEST_USERNAME=neo4j
NEO4J_TEST_DATABASE=neo4j

# Production Neo4j Database
NEO4J_PROD_URI=neo4j+s://your-prod-instance.databases.neo4j.io
NEO4J_PROD_PASSWORD=your-prod-password
NEO4J_PROD_USERNAME=neo4j
NEO4J_PROD_DATABASE=neo4j
```

## Branch Deployment Workflow

Each environment runs code from its dedicated branch:
- **Dev**: `dev` branch → pecha-backend-dev project
- **Test**: `test` branch → pecha-backend-test-3a4d0 project
- **Prod**: `main` branch → pecha-backend project

### Important: Always verify which branch you're on before deploying
```bash
git branch --show-current
```

### How to Deploy Different Branches

**Deploy to Dev Environment:**
```bash
git checkout dev
firebase deploy --only functions --project dev
```

**Deploy to Test Environment:**
```bash
git checkout test
firebase deploy --only functions --project test
```

**Deploy to Production:**
```bash
git checkout main
firebase deploy --only functions --project prod
```

### Environment Configuration

Each environment automatically uses:
- **Different Neo4j databases** (configured via Firebase secrets)
- **Different storage buckets** (auto-detected by project ID)
- **Different code versions** (from their respective git branches)

## API Endpoints

- **Dev**: https://api-l25bgmwqoa-uc.a.run.app
- **Test**: https://api-kwgjscy6gq-uc.a.run.app
- **Prod**: https://api-aq25662yyq-uc.a.run.app

## Documentation
Available at: https://pecha-backend.web.app/api

