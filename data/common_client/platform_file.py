#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2024 The community Authors.
# A-Tune is licensed under the Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#     http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR
# PURPOSE.
# See the Mulan PSL v2 for more details.
# Create: 2024/10/15
import yaml

from collect.api import request_url


class PlatformFile:

    @classmethod
    def get_yaml_file(cls, yaml_file, access_token):
        headers = {'Authorization': f'token {access_token}'}
        yaml_response = request_url(yaml_file, headers=headers, verify=False)
        if yaml_response.status_code != 200:
            print('Cannot fetch online yaml file.', yaml_response.text)
            return {}
        try:
            yaml_json = yaml.safe_load(yaml_response.text)
        except yaml.YAMLError as e:
            print(f'Error parsing YAML: {e}')
            return {}
        return yaml_json
