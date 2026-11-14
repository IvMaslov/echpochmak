# syntax=docker/dockerfile:1

# PyIceberg client for the HA tests. Dependencies are installed at build
# time, so tests never download anything. The test scripts are mounted.
FROM python:3.12.12-slim

RUN pip install --no-cache-dir "pyiceberg[pyarrow]==0.12.0"

WORKDIR /scripts

# Kept running; tests run scripts with docker exec.
CMD ["sleep", "infinity"]
