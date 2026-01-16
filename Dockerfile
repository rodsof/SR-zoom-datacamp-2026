FROM python:3.13.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /code
ENV PATH="/code/.venv/bin:$PATH"

COPY pyproject.toml .python-version uv.lock ./
RUN uv sync --locked

COPY ingest_green_data.py .
COPY green_tripdata_2025-11.parquet .

ENTRYPOINT ["python", "ingest_green_data.py"]