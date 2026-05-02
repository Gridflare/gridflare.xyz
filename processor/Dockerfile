FROM python:3.9
COPY requirements.txt requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt
WORKDIR /code
ENV PYTHONPATH /code
CMD hypercorn processor/processor-webserver:app --bind 0.0.0.0:5000 --access-logfile -
