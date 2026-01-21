# Steps to deploy to GCP Cloud Run:
docker build -t black-golf-research .
gcloud config set project pytutoring-dev
gcloud artifacts repositories create black-golf-research --repository-format=docker --location=us-east4 --description="Docker repository for SD fun facts app"
gcloud auth configure-docker us-east4-docker.pkg.dev
docker tag black-golf-research us-east4-docker.pkg.dev/pytutoring-dev/black-golf-research/black-golf-research-0
docker push us-east4-docker.pkg.dev/pytutoring-dev/black-golf-research/black-golf-research-0
gcloud beta run services add-iam-policy-binding --region=us-east4 --member="admin@mattashton.altostrat.com" --role=roles/run.invoker black-golf-research-0
gcloud run deploy black-golf-research-0 --image us-east4-docker.pkg.dev/pytutoring-dev/black-golf-research/black-golf-research-0:latest --platform managed --region us-east4