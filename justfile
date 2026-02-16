# linux/amd64 is intel and linux/arm64 is mac M1
platform := "linux/arm64"
branch := `git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "HEAD"`
job_name := "on-cli"
args := "--asset all --seasons 2025"
tag := "dev"
dbt_target := "dev"
dvc_remote := "http"
acquirer := "transfermarkt-scraper"
syncer := "sync-kaggle"
workflow := "sync-kaggle"

image_tag := replace(platform, "/", "-") + "-" + tag

# list available recipes
default:
    @just --list

# pull data from the cloud (cloudflare r2)
dvc_pull:
    dvc pull

docker_login_ecr:
    aws ecr get-login-password --region eu-west-1 | docker login --username AWS --password-stdin 272181418418.dkr.ecr.eu-west-1.amazonaws.com

docker_login_dockerhub:
    @echo $DOCKERHUB_TOKEN | docker login --username dcaribou --password-stdin

docker_login_flyio:
    flyctl auth docker

# build the project docker image and label it accordingly
docker_build:
    docker build --platform={{platform}} \
        -t dcaribou/transfermarkt-datasets:{{image_tag}} \
        .

docker_push_dockerhub: docker_build docker_login_dockerhub
    docker push dcaribou/transfermarkt-datasets:{{image_tag}}

docker_push_flyio: docker_login_flyio
    docker pull dcaribou/transfermarkt-datasets:{{image_tag}}
    docker tag \
        dcaribou/transfermarkt-datasets:{{image_tag}} \
        registry.fly.io/transfermarkt-datasets:{{image_tag}}
    docker push registry.fly.io/transfermarkt-datasets:{{image_tag}}

# run the acquiring process locally (refreshes data/raw)
acquire_local:
    scripts/runner.sh scripts/acquiring {{acquirer}} {{args}}

# run the acquiring process in a local docker
acquire_docker:
    docker run -ti \
        --env-file .env \
        -v `pwd`/.:/app/transfermarkt-datasets/ \
        --memory=4g \
        dcaribou/transfermarkt-datasets:dev \
            HEAD just prepare_local {{args}}

# run the prep process locally (refreshes data/prep)
prepare_local:
    mkdir -p data/prep
    cd dbt && dbt deps && dbt build --threads 4 --target {{dbt_target}}

# run the prep process in a local docker
prepare_docker:
    docker run -ti \
        --env-file .env \
        -v `pwd`/.:/app/transfermarkt-datasets/ \
        --memory=4g \
        dcaribou/transfermarkt-datasets:{{image_tag}} \
            {{branch}} just prepare_local

# run the prep process in the cloud (aws batch)
prepare_cloud job_definition_name="transfermarkt-datasets-batch-job-definition-dev":
    PYTHONPATH=$PYTHONPATH:`pwd`/. python scripts/prepare.py cloud \
        --branch {{branch}} \
        --job-name {{job_name}} \
        --job-definition {{job_definition_name}}

# run the sync process (refreshes data frontends)
sync:
    gunzip -r data/prep/*.csv.gz && \
    scripts/runner.sh scripts/synching {{syncer}} && \
    gzip -r data/prep/*.csv

# run streamlit app locally
streamlit_local:
    streamlit run streamlit/01_👋_about.py --server.port 8501 --browser.serverPort 8501

# run streamlit app in a local docker
streamlit_docker:
    docker run -ti \
        --env-file .env \
        dcaribou/transfermarkt-datasets:linux-amd64-{{tag}} \
        {{branch}} just streamlit_local

# deploy streamlit to app hosting service (fly.io)
streamlit_deploy: docker_push_flyio
    flyctl deploy

# run unit tests for core python module
test:
    pytest transfermarkt_datasets/tests

# run pre-commit hooks on all files
lint:
    .venv/bin/pre-commit run --all-files

act:
    act \
        "workflow_dispatch" \
        -s GITHUB_TOKEN \
        -j sync-dataworld \
        --pull=true \
        --no-skip-checkout \
        -W .github/workflows/sync-dataworld.yml \
        --container-architecture linux/amd64
