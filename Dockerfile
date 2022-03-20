FROM continuumio/miniconda3

WORKDIR /app

RUN apt-get update && \
    apt-get -y install gcc python3-dev jq awscli

COPY environment.yml /app/
RUN conda env create -f environment.yml

ENV PATH /opt/conda/envs/transfermarkt-datasets/bin:$PATH
RUN /bin/bash -c "source activate transfermarkt-datasets"

RUN git config --global user.email "transfermarkt-datasets-ci@transfermark-datasets.dev" && \
    git config --global user.name "CI Job" && \
    git config --global core.sshCommand "ssh -o StrictHostKeyChecking=no"

COPY bootstrap.sh /app/

ENTRYPOINT ["/bin/bash", "bootstrap.sh"]
