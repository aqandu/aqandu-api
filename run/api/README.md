# tetrad-api
The API source code for Tetrad

[View the API documentation](https://api.tetradsensors.com/docs)

# Running Locally
- Obtain a `config.json` file from one of the repo owners
- Set GCP credentials so you can access GCP resources locally. Get `clientid.json` from one of the repo owners
```bash
gcloud auth application-default login --client-id-file=<clientid.json>
```
- We use pipenv to manage python locally. Install `pipenv` on your system then use these tools:
- Install the local `pipenv` environment
```bash
pipenv install --skip-lock -r requirements.txt
```
- Run the application locally
```bash
pipenv run python main.py
```

# Deployment Notes
For deploying in Google Cloud Run.

- Enable Google Cloud Run in the GCP project
- Set your container name
```bash
    export GCP_CONTAINER="aqandu-api"
    # export GCP_CONTAINER="tetrad-api-$(git rev-parse HEAD | head -c 6)"
```
- Build the container and store it in gcr.io to fetch later for deployment
```bash
gcloud builds submit --tag gcr.io/$(gcloud config get-value project)/${GCP_CONTAINER} .
```
- Deploy the container
```bash
gcloud run deploy --image gcr.io/$(gcloud config get-value project)/${GCP_CONTAINER}:latest --region us-west2 --memory 4Gi --allow-unauthenticated --platform managed
```