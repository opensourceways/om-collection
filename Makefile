.EXPORT_ALL_VARIABLES:

VALIDATOR_IMAGE=swr.cn-north-1.myhuaweicloud.com/openeuler/validator:0.0.2

validator:
	GOFLAGS=-mod=vendor go build -o validator ./cmd/tools

build-image:
	docker build -t ${VALIDATOR_IMAGE} .
