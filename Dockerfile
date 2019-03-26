FROM python:3.6

RUN mkdir /falcon
WORKDIR /falcon

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

CMD ["gunicorn", "falcon.run:app", "-b 0.0.0.0:8000"]
