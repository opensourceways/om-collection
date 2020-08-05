FROM python:3.5

MAINTAINER zhongjun <jun.zhongjun2@gmail.com>
ENV LOG_DIR /var/log/om

RUN mkdir -p /var/lib/om
RUN mkdir -p ${LOG_DIR}
WORKDIR /var/lib/om

COPY ./george/ /var/lib/om

RUN apt-get update && \
    pip install --upgrade pip && \
    pip3 install -r requirements.txt


RUN wget https://github.com/huaweicloud/huaweicloud-sdk-python-obs/archive/v3.20.7.tar.gz && \
    tar -xvzf v3.20.7.tar.gz  && \
    cd huaweicloud-sdk-python-obs-3.20.7/src && python3 setup.py install

CMD python3 -u  george.py

