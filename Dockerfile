# syntax = docker/dockerfile:1.2

FROM python:3.9

WORKDIR /cltl-emissor-data
COPY src requirements.txt makefile ./
COPY config ./config
COPY util ./util

RUN --mount=type=bind,target=/cltl-emissor-data/repo,from=cltl/cltl-requirements:latest,source=/repo \
        make venv project_repo=/cltl-emissor-data/repo/leolani project_mirror=/cltl-emissor-data/repo/mirror

CMD . venv/bin/activate && python main.py
