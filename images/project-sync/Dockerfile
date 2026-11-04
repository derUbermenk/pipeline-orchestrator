FROM python:3.12-slim

RUN chmod g=u /etc/passwd
RUN apt-get update && apt-get install -y --no-install-recommends git openssh-client \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY src /src

WORKDIR /src

ENTRYPOINT ["python3", "project_sync.py"]