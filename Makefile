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

docker_login_ecr :
	aws ecr get-login-password --region eu-west-1 | docker login --username AWS --password-stdin 272181418418.dkr.ecr.eu-west-1.amazonaws.com

docker_login_dockerhub:
	echo ${DOCKERHUB_TOKEN} | docker login --username dcaribou --password-stdin

docker_login_flyio :
	fly auth docker

docker_build :
	docker build --platform=$(PLATFORM) \
		-t dcaribou/transfermarkt-datasets:$(IMAGE_TAG) \
		-t registry.fly.io/transfermarkt-datasets:$(IMAGE_TAG) \
		.

docker_push_dockerhub : docker_login_dockerhub
	docker push dcaribou/transfermarkt-datasets:$(IMAGE_TAG)

docker_push_flyio : docker_login_flyio
	docker push registry.fly.io/transfermarkt-datasets:$(IMAGE_TAG)

dvc_pull:
	dvc pull

stash_and_commit :
	dvc commit -f && git add data \
    git diff-index --quiet HEAD data || git commit -m "$(MESSAGE)" && \
    git push && dvc push

acquire_local :
	python 1_acquire.py local $(ARGS)

acquire_docker :
	docker run -ti \
			--env-file .env \
			-v `pwd`/.:/app/transfermarkt-datasets/ \
			--memory=4g  \
			dcaribou/transfermarkt-datasets:dev \
				python 1_acquire.py local $(ARGS)

acquire_cloud : JOB_DEFINITION_NAME = transfermarkt-datasets-batch-job-definition-dev
acquire_cloud :
	python 1_acquire.py cloud \
		--branch $(BRANCH) \
		--job-name $(JOB_NAME) \
		--job-definition $(JOB_DEFINITION_NAME) \
		ARGS='$(ARGS)' MESSAGE='$(MESSAGE)'

prepare_local :
	python -Wignore 2_prepare.py local $(ARGS)

prepare_docker :
	docker run -ti \
			--env-file .env \
			-v `pwd`/.:/app/transfermarkt-datasets/ \
			--memory=4g  \
			dcaribou/transfermarkt-datasets:dev \
				$(BRANCH) "prepared from local" 2_prepare.py local $(ARGS)

prepare_cloud : JOB_DEFINITION_NAME = transfermarkt-datasets-batch-job-definition-dev
prepare_cloud : 
	python 2_prepare.py cloud \
		--branch $(BRANCH) \
		--job-name $(JOB_NAME) \
		--job-definition $(JOB_DEFINITION_NAME) \
		MESSAGE='$(MESSAGE)'

sync : MESSAGE = Manual sync
sync :
	python 3_sync.py --message "$(MESSAGE)" --season 2022

streamlit_deploy : docker_push_flyio
	fly deploy

streamlit_local :
	streamlit run streamlit/01_👋_about.py

streamlit_docker :
	docker run -ti \
		--env-file .env \
		dcaribou/transfermarkt-datasets:linux-amd64-${TAG} \
		${BRANCH} make streamlit_local

streamlit_cloud :
	streamlit run streamlit/01_👋_about.py

dagit_local :
	dagit -f transfermarkt_datasets/dagster/jobs.py
