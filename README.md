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

## Running tests (pytest)
These integration tests use a real Neo4j instance.

Set these values in `functions/.env`:

```env
NEO4J_TEST_URI=bolt://localhost:7687
NEO4J_TEST_PASSWORD=your_password
```

Then run from the `functions` directory:

```bash
pytest
```
If pytest doesn't work try with
```bash
python -m pytest
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

Then restart the emulator:
```bash
firebase emulators:start --only functions
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

## Documentation
Available at: https://pecha-backend.web.app/api

