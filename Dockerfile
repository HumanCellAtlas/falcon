FROM python:3.6

RUN mkdir /falcon
WORKDIR /falcon

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

CMD ["python", "./falcon/__main__.py"]
