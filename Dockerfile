FROM python:3.6

RUN mkdir /falcon
WORKDIR /falcon

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

ENTRYPOINT ["python", "-um", "falcon.__main__"]
