FROM continuumio/miniconda3

WORKDIR /app

RUN apt-get update && \
    apt-get -y install gcc python3-dev jq awscli

RUN git clone https://github.com/dcaribou/transfermarkt-datasets.git
WORKDIR /app/transfermarkt-datasets

RUN conda env create -f environment.yml

ENV PATH /opt/conda/envs/transfermarkt-datasets/bin:$PATH
RUN /bin/bash -c "source activate transfermarkt-datasets"
RUN eval `ssh-agent`

ENTRYPOINT ["/bin/bash"]
