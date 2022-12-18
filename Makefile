PLATFORM = linux/arm64 # linux/amd64
BRANCH = $(shell git rev-parse --abbrev-ref HEAD)
JOB_NAME = on-cli

DASH:= -
SLASH:= /

# replace . with -
PLATFORM_TAG = $(subst $(SLASH),$(DASH),$(PLATFORM))

ecr_login :
	aws ecr get-login-password --region eu-west-1 | docker login --username AWS --password-stdin 272181418418.dkr.ecr.eu-west-1.amazonaws.com

build :
	docker build --platform=$(PLATFORM) -t transfermarkt-datasets-streamlit:$(PLATFORM_TAG) .

push :
	docker tag \
		transfermarkt-datasets-streamlit:linux-amd64 \
		registry.fly.io/transfermarkt-datasets:linux-amd64 && \
	docker push registry.fly.io/transfermarkt-datasets:linux-amd64

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
acquire_cloud : ARGS = --asset all --seasons 2022
acquire_cloud :
	python 1_acquire.py cloud \
		--branch $(BRANCH) \
		--job-name $(JOB_NAME) \
		--job-definition $(JOB_DEFINITION_NAME) \
		"$(ARGS)"

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
		""

sync : MESSAGE = Manual sync
sync :
	python 3_sync.py --message "$(MESSAGE)" --season 2022

streamlit_deploy :
	docker tag transfermarkt-datasets-streamlit registry.heroku.com/transfermarkt-datasets/web && \
	docker push registry.heroku.com/transfermarkt-datasets/web && \
	heroku container:release web

streamlit_local :
	streamlit run streamlit/01_👋_about.py

streamlit_docker :
	docker run -ti -e PORT=8085 \
		transfermarkt-datasets-streamlit:linux-arm64

streamlit_cloud :
	streamlit run streamlit/01_👋_about.py

dagit_local :
	dagit -f transfermarkt_datasets/dagster/jobs.py
