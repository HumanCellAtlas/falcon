FROM python:3.6

RUN mkdir /falcon
WORKDIR /falcon

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

ENTRYPOINT ["python", "-u", "falcon/__main__.py"]
