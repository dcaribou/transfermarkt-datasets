PLATFORM = linux/arm64 # linux/amd64
BRANCH = $(shell git rev-parse --abbrev-ref HEAD)

build : 
	docker build --platform=$(PLATFORM) -t dcaribou/transfermarkt-datasets:dev .

push :
	docker push dcaribou/transfermarkt-datasets:dev

COMMAND = 2_prepare.py
EXEC_COMMAND = $(shell PASSED_COMMAND="$(COMMAND)"; echo $${PASSED_COMMAND// /,})

run_local :
	docker run -ti \
		--env-file .env \
		-v `pwd`/.:/app/transfermarkt-datasets/ \
		--memory=4g  \
		dcaribou/transfermarkt-datasets:dev $(BRANCH) $(COMMAND)

run_bootstrap :
	docker run -ti \
		--env-file .env \
		--memory=4g  \
		dcaribou/transfermarkt-datasets:dev $(BRANCH) $(COMMAND)

run_batch : JOB_DEFINITION_NAME = transfermarkt-datasets-batch-job-definition-dev
run_batch : REVISION = $(shell \
  aws batch describe-job-definitions | \
  jq --arg jdname $(JOB_DEFINITION_NAME) '.jobDefinitions | map(select(.jobDefinitionName==$$jdname)) | sort_by(.revision) | last | .revision' \
)

run_batch : JOB_NAME = on-cli
run_batch : JOB_ID = $(shell \
	aws batch submit-job \
		--job-name $(JOB_NAME) \
		--job-queue transfermarkt-datasets-batch-compute-job-queue \
		--job-definition $(JOB_DEFINITION_NAME):$(REVISION) \
		--container-overrides \
			command=$(BRANCH),$(EXEC_COMMAND) \
	| jq -r '.jobId' \
)
run_batch:
	JOB_ID=$(JOB_ID); \
	JOB_STATUS=$$(aws batch describe-jobs --jobs $$JOB_ID | jq -r '.jobs[0].status'); \
	while [ $$JOB_STATUS != FAILED ] && [ $$JOB_STATUS != SUCCEEDED ]; do \
		sleep 5; \
		JOB_STATUS=$$(aws batch describe-jobs --jobs $$JOB_ID | jq -r '.jobs[0].status'); \
		echo $$JOB_STATUS; \
	done; \
	if [ $$JOB_STATUS = FAILED ]; then \
		exit 1; \
	else \
		exit 0; \
	fi
