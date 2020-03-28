FROM golang:stretch

COPY . /root/om-collections

RUN cd /root/om-collections && \
GOFLAGS=-mod=vendor go build -o validator ./cmd/tools && \
cp validator /usr/bin

