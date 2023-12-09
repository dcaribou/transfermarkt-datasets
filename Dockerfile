FROM python:3.8

WORKDIR /app

RUN apt-get update && \
    apt-get -y install \
    gcc python3-dev jq awscli nodejs \
    python3-launchpadlib tree

COPY pyproject.toml /app
COPY poetry.lock /app

ENV PYTHONPATH=${PYTHONPATH}:${PWD}

RUN pip3 install poetry
RUN poetry config virtualenvs.create false
RUN poetry install --no-dev

RUN /bin/bash -c "poetry shell"

RUN git config --global user.email "transfermarkt-datasets-ci@transfermark-datasets.dev" && \
    git config --global user.name "CI Job" && \
    git config --global core.sshCommand "ssh -o StrictHostKeyChecking=no"

# Creating folders, and files for a project:
COPY scripts/bootstrap.sh /app/

ENTRYPOINT ["/bin/bash", "bootstrap.sh"]
CMD ["master", "make", "streamlit_local"]
