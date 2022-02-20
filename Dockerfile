FROM continuumio/miniconda3

WORKDIR /app

RUN apt-get update && \
    apt-get -y install gcc python3-dev

# Create the environment:
COPY environment.yml .
RUN conda env create -f environment.yml

ENV PATH /opt/conda/envs/transfermarkt-datasets/bin:$PATH
RUN /bin/bash -c "source activate transfermarkt-datasets"
