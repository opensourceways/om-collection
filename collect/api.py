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
# Create: 2024/6/21
import threading

import requests


global_thread_info = threading.local()
retry_time = 3


def exception_handler(func):
    def warp(*args, **kwargs):
        try:
            # The first execution of the function, init retry num
            if not args or args[-1] != "retry":
                global_thread_info.num = 0
                global_thread_info.retrystate = 0
                new_arg = args
            else:
                # Remove parameter: retry
                new_arg = args[:-1]
            response = func(*new_arg, **kwargs)
        except requests.exceptions.RequestException as ex:
            while global_thread_info.num < retry_time and global_thread_info.retrystate == 0:
                try:
                    global_thread_info.num += 1
                    print("retry:" + str(global_thread_info.num) + " times")
                    print("error:" + str(ex))

                    # Add parameter: retry
                    if 'retry' not in args:
                        response = warp(*args, "retry", **kwargs)
                    else:
                        response = warp(*args, **kwargs)
                    return response
                finally:
                    pass
        except Exception as e:
            print("fetch error :" + str(e))
            print("retry: " + str(func.__name__) + ": " + str(global_thread_info.num) + " times")
            raise e
        else:
            # Retry succeed
            global_thread_info.retrystate = 1
            global_thread_info.num = 0
            return response
    return warp


@exception_handler
def request_url(url, payload=None, headers=None, method="GET", stream=False, verify=False, auth=None):
    if method == 'GET':
        response = requests.get(url, params=payload, headers=headers, stream=stream, auth=auth, verify=verify, timeout=60)
    else:
        response = requests.post(url, data=payload, headers=headers, stream=stream, verify=verify, auth=auth)
    return response
