# om-collections

Operation system for opensource community. such as: [openEuler](https://openeuler.org/zh/), [mindSpore](https://www.mindspore.cn/)

## Download
```
$ git clone https://gitee.com/opensourceway/om-collections
```

## Usage
```
 cd om-collections
 pip3 install -r requirement.txt
 python  george.py
```


## Example of config file: [config.ini](https://gitee.com/opensourceway/om-collections/blob/master/config.ini)

```
[general]
from_data=20200504
es_url=https://127.0.0.1:9200
sleep_time=28800
authorization=Basic xfdjgyttufhggfgfdfgdgfdhgfhgfhg

[baidutongji]
index_name=baidutongji
is_enterprise=true
username=baidutongji_user_name
password=xxxxxxxx
token=
site_id=37834763

[mailman]
index_name=maillist_user
mailman_core_endpoint=http://mailman-xcfdfsdf:8001/3.1
mailman_core_user=admin
mailman_core_password=password
mailman_core_domain_name=cokdfe.org
```


## Build docker image
```
git clone https://gitee.com/opensourceway/om-collections
cd ..
docker build -t om:0.0.2 .
docker tag om:0.0.2 swr.cn-north-4.myhuaweicloud.com/om/om-collection:0.0.2
docker push swr.cn-north-4.myhuaweicloud.com/om/om-collection:0.0.2
```

run docker image
```
docker run  -v /local_path/config.ini:/var/lib/om/config.ini -v /local_path/users:/var/lib/om/users  -d  om:0.0.2
```
