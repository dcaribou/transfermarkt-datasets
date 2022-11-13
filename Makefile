PLATFORM = linux/arm64 # linux/amd64
BRANCH = $(shell git rev-parse --abbrev-ref HEAD)
JOB_NAME = on-cli

test :
	pytest -k "not build_all"

build :
	docker build \
		--platform=$(PLATFORM) \
		--tag dcaribou/transfermarkt-datasets:dev \
		--tag registry.heroku.com/transfermarkt-datasets/web \
		.

push :
	docker push dcaribou/transfermarkt-datasets:dev

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
	docker push registry.heroku.com/transfermarkt-datasets/web && \
	heroku container:release web

streamlit_local :
	streamlit run streamlit/01_👋_about.py

streamlit_cloud :
	streamlit run \
		--server.port ${PORT} \
		streamlit/01_👋_about.py

dagit_local :
	dagit -f transfermarkt_datasets/dagster/jobs.py
