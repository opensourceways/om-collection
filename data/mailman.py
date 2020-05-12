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


import os
import signal
from mailmanclient import Client
from http.server import SimpleHTTPRequestHandler, HTTPServer
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from data import common
from data.common import ESClient


class MailMan(object):
    def __init__(self, config=None):
        self.config = config
        self.endpoint = config.get('mailman_core_endpoint')
        self.user = config.get('mailman_core_user')
        self.password = config.get('mailman_core_password')
        self.domain_name = config.get('mailman_core_domain_name')
        self.url = config.get('es_url')
        self.index_name = config.get('index_name')
        self.esClient = ESClient(config)


    def run(self, from_date=None):
        self.prepare_list()

    def prepare_list(self):
        # pre-check before handling mailman core service
        if self.domain_name == "":
            print("Must specify 'domain_name' for mail list preparation.")
            return

        client = Client(self.endpoint, self.user, self.password)

        actions = []
        all_emails = []
        default_domain = client.get_domain(self.domain_name)
        print(default_domain)
        for mlist in client.lists:
            print(mlist.fqdn_listname)
            list_hand = client.get_list(mlist.fqdn_listname)
            print(list_hand)

            for member in list_hand.members:
               member_email = str(member).split( )[1].split('"')[1].split('"')[0]
               m = list_hand.get_member(member_email)

               user = m.user
               for address in user.addresses:
                   body = {
                        "user_name": user.display_name,
                        "maillist_domain": address.email.split('@')[1],
                        "author_domain": address.email.split('@')[1],
                        "maillist": mlist.fqdn_listname,
                        "role": m.role,
                        "user_address": address.email,
                        "user_display_name": user.display_name,
                        "address_email": address.email,
                        "address_original_email": address.original_email,
                        "address_display_name": address.display_name,
                        "user_id": user.user_id,
                        "is_server_owner": user.is_server_owner,
                        "registeredTime": address.registered_on.split('.')[0],
                        "created_at": user.created_on.split('.')[0],
                        "updated_at": user.created_on.split('.')[0],
                   }
                   id = suser.user_id + address.email
                   all_emails.append(address.email)
                   action = common.getSingleAction(self.index_name, id, body)
                   actions += action

        for suser in client.users:
            for address in suser.addresses:
                if address.email in all_emails:
                    continue
                print(address)
                body = {
                     "user_name": suser.display_name,
                     "maillist_domain": address.email.split('@')[1],
                     "author_domain": address.email.split('@')[1],
                     "user_address": address.email,
                     "user_display_name": suser.display_name,
                     "address_email": address.email,
                     "address_original_email": address.original_email,
                     "address_display_name": address.display_name,
                     "user_id": suser.user_id,
                     "is_server_owner": suser.is_server_owner,
                     "registeredTime": address.registered_on.split('.')[0],
                     "createTime": suser.created_on.split('.')[0],
                     "updateTime": suser.created_on.split('.')[0],
                }
                id = suser.user_id + address.email
                all_emails.append(address.email)
                action = common.getSingleAction(self.index_name, id, body)
                actions += action

        self.esClient.safe_put_bulk(actions)
        print("All mailman thread finished")

