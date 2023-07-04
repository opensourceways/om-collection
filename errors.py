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


class BaseError(Exception):
    """Base class for Collection exceptions.

    Derived classes can overwrite the error message declaring ``message``
    property.
    """
    message = 'Collection base error'

    def __init__(self, **kwargs):
        super().__init__()
        self.msg = self.message % kwargs

    def __str__(self):
        return self.msg


class ArchiveError(BaseError):
    """Generic error for archive objects"""

    message = "%(cause)s"


class ArchiveManagerError(BaseError):
    """Generic error for archive manager"""

    message = "%(cause)s"


class BackendError(BaseError):
    """Generic error for backends"""

    message = "%(cause)s"


class HttpClientError(BaseError):
    """Generic error for HTTP Cient"""

    message = "%(cause)s"


class RepositoryError(BaseError):
    """Generic error for repositories"""

    message = "%(cause)s"


class RateLimitError(BaseError):
    """Exception raised when the rate limit is exceeded"""

    message = "%(cause)s; %(seconds_to_reset)s seconds to rate reset"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seconds_to_reset = kwargs['seconds_to_reset']

    @property
    def seconds_to_reset(self):
        return self._seconds_to_reset


class ParseError(BaseError):
    """Exception raised a parsing errors occurs"""

    message = "%(cause)s"


class BackendCommandArgumentParserError(BaseError):
    """Generic error for BackendCommandArgumentParser"""

    message = "%(cause)s"
