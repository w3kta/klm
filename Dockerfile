FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir polars
COPY solution.py . 
COPY data/ ./data/
CMD ["python", "solution.py"]
CMD ["python", "solution.py"]
