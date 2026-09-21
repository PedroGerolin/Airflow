FROM python:3.12-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

# dependencias primeiro: a camada so refaz quando requirements.txt muda
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py repo.py ./
COPY tests ./tests

EXPOSE 8501
HEALTHCHECK CMD python -c "import urllib.request as u; u.urlopen('http://localhost:8501/_stcore/health')"
CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501", "--browser.gatherUsageStats=false"]
