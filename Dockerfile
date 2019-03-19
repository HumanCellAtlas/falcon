FROM python:3.6

RUN mkdir /falcon
WORKDIR /falcon

COPY . .

RUN pip install -U setuptools

RUN pip install .

CMD ["python", "./falcon/__main__.py"]
