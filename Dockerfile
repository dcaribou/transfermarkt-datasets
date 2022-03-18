FROM continuumio/miniconda3

COPY . /app
COPY .git /app/.git

WORKDIR /app

RUN apt-get update && \
    apt-get -y install gcc python3-dev jq awscli

RUN conda env create -f environment.yml

ENV PATH /opt/conda/envs/transfermarkt-datasets/bin:$PATH
RUN /bin/bash -c "source activate transfermarkt-datasets"

ENTRYPOINT ["/bin/bash", "-c"]
