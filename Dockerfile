FROM python:3.7

MAINTAINER zhongjun <jun.zhongjun2@gmail.com>
ENV LOG_DIR /var/log/om

RUN mkdir -p /var/lib/om
RUN mkdir -p ${LOG_DIR}
WORKDIR /var/lib/om

COPY ./om-collections/ /var/lib/om

RUN apt-get update && \
    pip install --upgrade pip && \
    pip3 install -r requirements.txt


RUN wget https://github.com/huaweicloud/huaweicloud-sdk-python-obs/archive/v3.20.7.tar.gz && \
    tar -xvzf v3.20.7.tar.gz  && \
    cd huaweicloud-sdk-python-obs-3.20.7/src && python3 setup.py install

RUN wget https://github.com/huaweicloud/huaweicloud-sdk-python/archive/v1.0.24.tar.gz && \
    tar -xvzf v1.0.24.tar.gz && \
    cd huaweicloud-sdk-python-1.0.24  &&\
    pip3 install -r requirements.txt &&\
    python3 setup.py install 

RUN pip3 install mailmanclient==3.1

CMD python3 -u  george.py

