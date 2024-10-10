
FROM harbor.containers.wurnet.nl/proxy-cache/library/python:3.8-slim-buster
LABEL maintainer="genuchten@yahoo.com"

RUN apt-get update && apt-get install --yes \
        ca-certificates libexpat1 \
    && rm -rf /var/lib/apt/lists/*

RUN adduser --uid 1000 --gecos '' --disabled-password metadata

ENV POSTGRES_HOST=host.docker.internal
ENV POSTGRES_PORT=5432
ENV POSTGRES_DB=postgres
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=******

WORKDIR /home/metadata

RUN chown --recursive metadata:metadata .

# initially copy only the requirements files
COPY --chown=metadata \
    requirements.txt \
    ./

RUN pip install -U pip && \
    python3 -m pip install \
    -r requirements.txt
      

COPY --chown=metadata . .

WORKDIR /home/metadata/translation

EXPOSE 8000

USER metadata


ENTRYPOINT [ "python3", "-m", "uvicorn", "api:app", "--reload", "--host", "0.0.0.0", "--port", "8000" ]