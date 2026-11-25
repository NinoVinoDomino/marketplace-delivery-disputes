FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY resolveflow/ resolveflow/
COPY policies/ policies/
COPY evals/ evals/
COPY tests/ tests/
RUN python -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. \
        resolveflow/proto/guardrails.proto resolveflow/proto/disputes.proto \
    && mkdir /data \
    && chown 10001:10001 /data

USER 10001:10001
CMD ["python", "-m", "resolveflow.server"]
