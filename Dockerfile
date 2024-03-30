FROM openeuler/openeuler:22.03

MAINTAINER zhongjun <jun.zhongjun2@gmail.com>
ENV LOG_DIR /var/log/om

RUN mkdir -p /var/lib/om
RUN mkdir -p ${LOG_DIR}
WORKDIR /var/lib/om

COPY . /var/lib/om

RUN yum update -y \
    && yum install -y shadow wget git rsync

RUN wget https://repo.huaweicloud.com/python/3.7.17/Python-3.7.17.tgz \
    && tar -zxvf Python-3.7.17.tgz \
    && cd Python-3.7.17 \
    && yum install -y gcc libffi-devel zlib* openssl-devel make \
    && ./configure --prefix=/usr/local/python3 \
    && make && make install

RUN cd /usr/bin \
    && rm -rf ./python3 \
    && ln -s /usr/local/python3/bin/python3 /usr/bin/python3 \
    && ln -s /usr/local/python3/bin/pip3 /usr/bin/pip3

RUN python3 -m pip install --upgrade pip \
    && pip3 config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple \
    && pip3 install -r requirements.txt \
    && pip3 install google-api-python-client \
    && pip3 install --upgrade oauth2client

RUN wget -P /var/lib/ https://github.com/AlDanial/cloc/releases/download/v1.94/cloc-1.94.tar.gz && \
    cd /var/lib/ && \
    tar -zxvf cloc-1.94.tar.gz

RUN wget https://github.com/huaweicloud/huaweicloud-sdk-python-obs/archive/v3.20.7.tar.gz && \
    tar -xvzf v3.20.7.tar.gz  && \
    cd huaweicloud-sdk-python-obs-3.20.7/src && python3 setup.py install

RUN wget https://github.com/huaweicloud/huaweicloud-sdk-python/archive/v1.0.24.tar.gz && \
    tar -xvzf v1.0.24.tar.gz && \
    cd huaweicloud-sdk-python-1.0.24  &&\
    pip3 install -r requirements.txt &&\
    python3 setup.py install

CMD python3 -u  george.py