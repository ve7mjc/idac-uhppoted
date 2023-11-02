FROM python:3.10-alpine

COPY requirements/common.txt .

RUN pip install --no-cache-dir -r common.txt

WORKDIR /app

COPY app/. /app

CMD ["python", "main.py"]
