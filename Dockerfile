FROM python:3.8

WORKDIR /app

COPY ./requirements.txt .
RUN pip install -r requirements.txt --no-cache-dir

COPY ./app .
COPY ./model ./model

EXPOSE 30000

CMD ["gunicorn", "main:app", "--workers", "3", "--worker-class", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:30000", "--timeout", "120"]







# ENV MODEL_PATH /app/model/model.pkl
# ENV VECTORIZER_PATH /app/model/vectorizer.pkl

#CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "30000"]
#uvicorn main:app --host 0.0.0.0 --port 30000 --reload

#gunicorn main:app --workers 3 --worker-class uvicorn.workers.UvicornWorker --bind 0.0.0.0:30000 --timeout 120

#docker build -t tsc .
#docker run -d -p 30005:30000 --name tsc tsc
#docker-compose up --build
#docker-compose up -d --build
#docker-compose up -d --build --force-recreate

