FROM continuumio/miniconda3

COPY . /app
COPY .git /app/.git

WORKDIR /app

RUN apt-get update && \
    apt-get -y install gcc python3-dev jq awscli

RUN conda env create -f environment.yml

ENV PATH /opt/conda/envs/transfermarkt-datasets/bin:$PATH
RUN /bin/bash -c "source activate transfermarkt-datasets"

RUN git config --global user.email "transfermarkt-ci@transfermark-datasets.com" && \
    git config --global user.name "CI Bot"

ENTRYPOINT ["/bin/bash", "-c"]
