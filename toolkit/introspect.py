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

"""Common functions for introspection.

This module provides a set of functions that come in handy for
instrospection on a higher level than the provided by other libraries
such as `inspect`.
"""

import inspect


__all__ = [
    "inspect_signature_parameters",
    "find_signature_parameters",
    "find_class_properties"
]


def inspect_signature_parameters(callable_, excluded=None):
    """Get the parameters of a callable.

    Returns a list with the signature parameters of `callable_`.
    Parameters contained in `excluded` tuple will not be included
    in the result.

    :param callable_: callable object
    :param excluded: tuple with default parameters to exclude

    :result: list of parameters
    """
    if not excluded:
        excluded = ()

    signature = inspect.signature(callable_)
    params = [
        v for p, v in signature.parameters.items()
        if p not in excluded
    ]
    return params


def find_signature_parameters(callable_, candidates,
                              excluded=('self', 'cls')):
    """Find on a set of candidates the parameters needed to execute a callable.

    Returns a dictionary with the `candidates` found on `callable_`.
    When any of the required parameters of a callable is not found,
    it raises a `AttributeError` exception. A signature parameter
    whitout a default value is considered as required.

    :param callable_: callable object
    :param candidates: dict with the possible parameters to use
        with the callable
    :param excluded: tuple with default parameters to exclude

    :result: dict of parameters ready to use with the callable

    :raises AttributeError: when any of the required parameters for
        executing a callable is not found in `candidates`
    """
    signature_params = inspect_signature_parameters(callable_,
                                                    excluded=excluded)
    exec_params = {}

    add_all = False
    for param in signature_params:
        name = param.name

        if str(param).startswith('*'):
            add_all = True
        elif name in candidates:
            exec_params[name] = candidates[name]
        elif param.default == inspect.Parameter.empty:
            msg = "required argument %s not found" % name
            raise AttributeError(msg, name)
        else:
            continue

    if add_all:
        exec_params = candidates

    return exec_params


def find_class_properties(cls):
    """Find property members in a class.

    Returns all the property members of a class in a list of
    (name, value) pairs. Only those members defined with `property`
    decorator will be included in the list.

    :param cls: class where property members will be searched

    :returns: list of properties
    """
    candidates = inspect.getmembers(cls, inspect.isdatadescriptor)
    result = [
        (name, value) for name, value in candidates
        if isinstance(value, property)
    ]
    return result
