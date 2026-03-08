FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src/ ./src/
COPY tests/ ./tests/
# по умолчанию запускает fetch
CMD ["python", "src/main.py", "fetch"]
