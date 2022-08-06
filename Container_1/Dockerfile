FROM ubuntu:latest

RUN set -xe \
    && apt-get update \
    && apt-get install python3-pip -y
RUN pip install --upgrade pip
RUN apt install python3 -y
RUN pip install kafka-python
RUN pip install pytz

WORKDIR /usr/app/src

COPY producer_incorrect_data.py ./

#CMD ["python3", "./producer_incorrect_data.py"]