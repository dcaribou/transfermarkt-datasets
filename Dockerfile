FROM continuumio/miniconda3

WORKDIR /app

# The code to run when container is started:
COPY prep prep
COPY 1_acquire.py .
COPY 2_prepare.py .

# Create the environment:
COPY environment.yml .
RUN conda env create -f environment.yml

ENV PATH /opt/conda/envs/transfermarkt-datasets/bin:$PATH
RUN /bin/bash -c "source activate transfermarkt-datasets"
