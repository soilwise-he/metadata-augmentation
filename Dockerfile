
FROM python:3.12-slim-trixie
LABEL maintainer="genuchten@yahoo.com"

RUN apt-get update && apt-get install --yes \
        ca-certificates libexpat1 \
    && rm -rf /var/lib/apt/lists/*


ENV POSTGRES_HOST=host.docker.internal
ENV POSTGRES_PORT=5432
ENV POSTGRES_DB=postgres
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=******

WORKDIR /home/metadata

# initially copy only the requirements files
COPY requirements.txt .

RUN pip install -U pip && \
    python3 -m pip install \
    -r requirements.txt
      
COPY . .

ENTRYPOINT [ "" ]