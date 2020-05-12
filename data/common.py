#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2020 Technologies Co., Ltd.
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

import urllib3
urllib3.disable_warnings()

import requests
from requests.auth import HTTPBasicAuth



def safe_put_bulk(bulk_json, header, url):
    """Bulk items to a target index `url`. In case of UnicodeEncodeError,
    the bulk is encoded with iso-8859-1.

    :param url: target index where to bulk the items
    :param bulk_json: str representation of the items to upload
    """
    if not bulk_json:
        return
    header["Content-Type"] = "application/x-ndjson"

    try:
        res = requests.post(url + "/_bulk", data=bulk_json, headers=header,
                            verify=False)
        res.raise_for_status()
    except UnicodeEncodeError:
        # Related to body.encode('iso-8859-1'). mbox data
        logger.warning("Encondig error ... converting bulk to iso-8859-1")
        bulk_json = bulk_json.encode('iso-8859-1', 'ignore')
        res = requests.put(url, data=bulk_json, headers=headers)
        res.raise_for_status()


def getStartTime(index_name):
    # 2020-04-29T15:59:59.000Z
    last_time = getLastTime(index_name)
    # 20200429
    last_time = last_time.split("T")[0].replace("-", "")

    f = datetime.strptime(last_time, "%Y%m%d") + timedelta(days=1)
    # 20200430
    starTime = f.strftime("%Y%m%d")
    return starTime


def getLastTime(index_name):
    data_agg = '''
            "aggs": {
                "1": {
                  "max": {
                    "field": "created_at"
                  }
                }
            }
        '''

    data_json = '''
        { "size": 0, %s
        } ''' % data_agg
    res = es.search(index=index_name, body=data_json)
    result_num = res['hits']['total']['value']
    if result_num == 0:
        return
    # get min create at value
    created_at_value = res['aggregations']['1']['value_as_string']
    return created_at_value


def get_last_item_field(field, filters_=[], offset=False):
    """Find the offset/date of the last item stored in the index.
    """
    last_value = None

    if filters_ is None:
        filters_ = []

    terms = []
    for filter_ in filters_:
        if not filter_:
            continue
        term = '''{"term" : { "%s" : "%s"}}''' % (
        filter_['name'], filter_['value'])
        terms.append(term)

    data_query = '''"query": {"bool": {"filter": [%s]}},''' % (
        ','.join(terms))

    data_agg = '''
        "aggs": {
            "1": {
              "max": {
                "field": "%s"
              }
            }
        }
    ''' % field

    data_json = '''
    { "size": 0, %s  %s
    } ''' % (data_query, data_agg)

    print(data_json)
    res = es.search(index=index_name, body=data_json)

    if "value_as_string" in res["aggregations"]["1"]:
        last_value = res["aggregations"]["1"]["value_as_string"]
        last_value = str_to_datetime(last_value)
    else:
        last_value = res["aggregations"]["1"]["value"]
        if last_value:
            try:
                last_value = unixtime_to_datetime(last_value)
            except InvalidDateError:
                last_value = unixtime_to_datetime(last_value / 1000)

    return last_value


def get_last_date(field, filters_=[]):
    """Find the date of the last item stored in the index
    """
    last_date = get_last_item_field(field, filters_=filters_)

    return last_date


def get_incremental_date():
    """Field with the date used for incremental analysis."""

    return "updated_at"


def get_last_update_from_es(_filters=[]):
    last_update = get_last_date(get_incremental_date(), _filters)

    return last_update


def get_date(time):
    if time:
        return time.split("+")[0]
    else:
        return None


def get_time_to_first_attention(item):
    """Get the first date at which a comment or reaction was made to the issue by someone
    other than the user who created the issue
    """
    comment_dates = [str_to_datetime(comment['created_at']) for comment in item['comments_data']
                     if item['user']['login'] != comment['user']['login']]
    if comment_dates:
        return min(comment_dates)
    return None


def get_first_contribute_at(user, current_date):
    if user in gloabl_item:
        if str_to_datetime(current_date) < str_to_datetime(gloabl_item[user]):
            gloabl_item[user] = current_date
            return current_date
        else:
            return None
    else:
        gloabl_item[user] = current_date
        return current_date


def get_time_diff_days(start, end):
    ''' Number of days between two dates in UTC format  '''

    if start is None or end is None:
        return None

    if type(start) is not datetime.datetime:
        start = dateutil.parser.parse(start).replace(tzinfo=None)
    if type(end) is not datetime.datetime:
        end = dateutil.parser.parse(end).replace(tzinfo=None)

    seconds_day = float(60 * 60 * 24)
    diff_days = (end - start).total_seconds() / seconds_day
    diff_days = float('%.2f' % diff_days)

    return diff_days


def datetime_utcnow():
    """Handy function which returns the current date and time in UTC."""
    return datetime.datetime.now()


def str_to_datetime(ts):
    """Format a string to a datetime object.
    This functions supports several date formats like YYYY-MM-DD,
    MM-DD-YYYY, YY-MM-DD, YYYY-MM-DD HH:mm:SS +HH:MM, among others.
    When the timezone is not provided, UTC+0 will be set as default
    (using `dateutil.tz.tzutc` object).
    :param ts: string to convert
    :returns: a datetime object
    :raises IvalidDateError: when the given string cannot be converted
        on a valid date
    """
    def parse_datetime(ts):
        dt = dateutil.parser.parse(ts)
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=dateutil.tz.tzutc())
        return dt

    if not ts:
        raise InvalidDateError(date=str(ts))

    try:
        # Try to remove additional information after
        # timezone section because it cannot be parsed,
        # like in 'Wed, 26 Oct 2005 15:20:32 -0100 (GMT+1)'
        # or in 'Thu, 14 Aug 2008 02:07:59 +0200 CEST'.
        m = re.search(r"^.+?\s+[\+\-\d]\d{4}(\s+.+)$", ts)
        if m:
            ts = ts[:m.start(1)]

        try:
            dt = parse_datetime(ts)
        except ValueError as e:
            # Try to remove the timezone, usually it causes
            # problems.
            m = re.search(r"^(.+?)\s+[\+\-\d]\d{4}.*$", ts)

            if m:
                dt = parse_datetime(m.group(1))
                print("Date %s does not have a valid timezone. "
                               "Date converted removing timezone info"% ts)
                return dt

            raise e

        try:
            # Check that the offset is between -timedelta(hours=24) and
            # timedelta(hours=24). If it is not the case, convert the
            # date to UTC and remove the timezone info.
            _ = dt.astimezone(dateutil.tz.tzutc())
        except ValueError:
            print("Date %s does not have a valid timezone; timedelta not in range. "
                           "Date converted to UTC removing timezone info" % ts)
            dt = dt.replace(tzinfo=dateutil.tz.tzutc()).astimezone(dateutil.tz.tzutc())

        return dt

    except ValueError as e:
        raise InvalidDateError(date=str(ts))
