FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY niru ./niru
COPY main.py ./
COPY config.yaml ./

RUN pip install --no-cache-dir .

CMD ["python", "main.py"]
