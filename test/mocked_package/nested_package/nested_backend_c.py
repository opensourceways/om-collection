#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 The community Authors.
# A-Tune is licensed under the Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#     http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR
# PURPOSE.
# See the Mulan PSL v2 for more details.
# Create: 2022-03
#
# Authors:
#     Valerio Cosentino <valcos@.com>
#

from collection.backend import (Backend,
                              BackendCommand)


class BackendC(Backend):
    """Mocked backend class used for testing"""

    def __init__(self, origin, tag=None, archive=None):
        super().__init__(origin, tag=tag, archive=archive)


class BackendCommandC(BackendCommand):
    """Mocked backend command class used for testing"""

    BACKEND = BackendC

    def __init__(self, *args):
        super().__init__(*args)
