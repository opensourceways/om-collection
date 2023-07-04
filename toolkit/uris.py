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
# Create: 2020-05
#

"""Functions for handling URIs."""


__all__ = ["urijoin"]


def urijoin(*args):
    """Joins given arguments into a URI.

    Trailing and leading slashes are stripped for each argument.

    This code is based on a Rune Kaagaard's answer on Stack Overflow.
    See http://stackoverflow.com/questions/1793261 for more into. The
    code was licensed as cc by-sa 3.0.

    :params *args: list of arguments to join

    :returns: a URI string
    """
    return '/'.join(map(lambda x: str(x).strip('/'), args))
