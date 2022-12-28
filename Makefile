# linux/amd64 is intel and linux/arm64 is mac M1
PLATFORM = linux/arm64
BRANCH = $(shell git rev-parse --abbrev-ref HEAD)
JOB_NAME = on-cli
ARGS = --asset all --seasons 2022
MESSAGE = some message
TAG = dev

DASH:= -
SLASH:= /

# replace . with -
IMAGE_TAG = $(subst $(SLASH),$(DASH),$(PLATFORM))-${TAG}

# https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
.PHONY: help
.DEFAULT_GOAL := help

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

dvc_pull: ## pull data from the cloud (aws s3)
	dvc pull

docker_login_ecr :
	aws ecr get-login-password --region eu-west-1 | docker login --username AWS --password-stdin 272181418418.dkr.ecr.eu-west-1.amazonaws.com

docker_login_dockerhub :
	@echo ${DOCKERHUB_TOKEN} | docker login --username dcaribou --password-stdin

docker_login_flyio :
	fly auth docker

docker_build: ## build the project docker image and label it accordingly
	docker build --platform=$(PLATFORM) \
		-t dcaribou/transfermarkt-datasets:$(IMAGE_TAG) \
		-t registry.fly.io/transfermarkt-datasets:$(IMAGE_TAG) \
		.

docker_push_dockerhub : docker_build docker_login_dockerhub
	docker push dcaribou/transfermarkt-datasets:$(IMAGE_TAG)

docker_push_flyio : docker_build docker_login_flyio
	docker push registry.fly.io/transfermarkt-datasets:$(IMAGE_TAG)

acquire_local: ## run the acquiring process locally (refreshes data/raw)
	PYTHONPATH=$(PYTHONPATH):`pwd`/. python scripts/acquire.py local $(ARGS)

acquire_docker: ## run the acquiring process in a local docker
	docker run -ti \
			--env-file .env \
			-v `pwd`/.:/app/transfermarkt-datasets/ \
			--memory=4g  \
			dcaribou/transfermarkt-datasets:dev \
				HEAD make prepare_local $(ARGS)

acquire_cloud: ## run the acquiring process in the cloud (aws batch)
acquire_cloud: JOB_DEFINITION_NAME = transfermarkt-datasets-batch-job-definition-dev
acquire_cloud:
	PYTHONPATH=$(PYTHONPATH):`pwd`/. python scripts/acquire.py cloud \
		--branch $(BRANCH) \
		--job-name $(JOB_NAME) \
		--job-definition $(JOB_DEFINITION_NAME) \
		ARGS='$(ARGS)' MESSAGE='$(MESSAGE)'

prepare_local: ## run the prep process locally (refreshes data/prep)
	PYTHONPATH=$(PYTHONPATH):`pwd`/. python -Wignore scripts/prepare.py local

prepare_docker: ## run the prep process in a local docker
	docker run -ti \
			--env-file .env \
			-v `pwd`/.:/app/transfermarkt-datasets/ \
			--memory=4g  \
			dcaribou/transfermarkt-datasets:dev \
				$(BRANCH) "prepared from local" prepare.py local $(ARGS)

prepare_cloud: ## run the prep process in the cloud (aws batch)
prepare_cloud: JOB_DEFINITION_NAME = transfermarkt-datasets-batch-job-definition-dev
prepare_cloud: 
	PYTHONPATH=$(PYTHONPATH):`pwd`/. python scripts/prepare.py cloud \
		--branch $(BRANCH) \
		--job-name $(JOB_NAME) \
		--job-definition $(JOB_DEFINITION_NAME) \
		MESSAGE='$(MESSAGE)'

sync: ## run the sync process (refreshes data frontends)
sync: MESSAGE = Manual sync
sync:
	PYTHONPATH=$(PYTHONPATH):`pwd`/. python scripts/sync.py --message "$(MESSAGE)" --season 2022

streamlit_local: ## run streamlit app locally
	streamlit run streamlit/01_👋_about.py

streamlit_docker: ## run streamlit app in a local docker
	docker run -ti \
		--env-file .env \
		dcaribou/transfermarkt-datasets:linux-amd64-${TAG} \
		${BRANCH} make streamlit_local

streamlit_deploy: ## deploy streamlit to app hosting service (fly.io)
streamlit_deploy: docker_push_flyio
	fly deploy

dagit_local: ## run dagit locally
	dagit -f transfermarkt_datasets/dagster/jobs.py

stash_and_commit: ## commit and push code and data
	dvc commit -f && git add data && \
    git diff-index --quiet HEAD data || git commit -m "$(MESSAGE)" && \
    git push && dvc push
