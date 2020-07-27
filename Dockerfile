FROM python:3.5

MAINTAINER zhongjun <jun.zhongjun2@gmail.com>
ENV LOG_DIR /var/log/om

RUN mkdir -p /var/lib/om
RUN mkdir -p ${LOG_DIR}
WORKDIR /var/lib/om

COPY ./george/ /var/lib/om

RUN apt-get update

CMD python3 -u  george.py

