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
import time
from mailmanclient import Client
from http.server import SimpleHTTPRequestHandler, HTTPServer

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
        startTime = time.time()
        print("Collect download data from maillist: staring")

        self.prepare_list()

        endTime = time.time()
        spent_time = time.strftime("%H:%M:%S",
                                   time.gmtime(endTime - startTime))
        print("Collect download data from maillist:"
              " finished after (%s)" % (spent_time))


    def prepare_list(self):
        email_orgs_dict = self.esClient.getOrgByEmail()
        # pre-check before handling mailman core service
        if self.domain_name == "":
            print("Must specify 'domain_name' for mail list preparation.")
            return

        print("Start to get mailistclient, Domain name:", self.domain_name)
        client = Client(self.endpoint, self.user, self.password)
        print("Start to get mailistclient success.")

        actions = ""
        all_emails = []
        default_domain = client.get_domain(self.domain_name)
        print(default_domain)

        i = 0
        for mlist in client.lists:
            print(mlist.fqdn_listname)
            list_hand = client.get_list(mlist.fqdn_listname)
            print(list_hand)

            for member in list_hand.members:
                member_email = str(member).split( )[1].split('"')[1].split('"')[0]
                m = list_hand.get_member(member_email)

                user = m.user

                for address in user.addresses:
                    print("member maillist adress %s,  num= %d" % (address.email, i))
                    company = 'independent'
                    if email_orgs_dict and address.email in email_orgs_dict:
                        company = email_orgs_dict[address.email]
                    body = {
                        "user_name": user.display_name,
                        "email_domain": address.email.split('@')[1],
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
                        "registeredTime": address.registered_on.split('.')[0] + "+08:00",
                        "created_at": user.created_on.split('.')[0] + "+08:00",
                        "updated_at": user.created_on.split('.')[0] + "+08:00",
                        "tag_user_company": company,
                    }
                    id = user.user_id + address.email + mlist.fqdn_listname

                    action = common.getSingleAction(self.index_name, id, body)
                    actions += action

                    if address.email not in all_emails:
                        i += 1
                    all_emails.append(address.email)


        self.esClient.safe_put_bulk(actions)

        actions = ""
        for suser in client.users:
            for address in suser.addresses:
                if address.email in all_emails:
                    continue
                print(address)
                print("maillist adress %s,  num= %d" % (
                address.email, i))
                company = 'independent'
                if email_orgs_dict and address.email in email_orgs_dict:
                    company = email_orgs_dict[address.email]
                body = {
                     "user_name": suser.display_name,
                     "email_domain": address.email.split('@')[1],
                     "maillist": None,
                     "role": None,
                     "author_domain": address.email.split('@')[1],
                     "user_address": address.email,
                     "user_display_name": suser.display_name,
                     "address_email": address.email,
                     "address_original_email": address.original_email,
                     "address_display_name": address.display_name,
                     "user_id": suser.user_id,
                     "is_server_owner": suser.is_server_owner,
                     "registeredTime": address.registered_on.split('.')[0] + "+08:00",
                     "created_at": suser.created_on.split('.')[0] + "+08:00",
                     "updated_at": suser.created_on.split('.')[0] + "+08:00",
                     "tag_user_company": company,
                }
                id = suser.user_id + address.email

                action = common.getSingleAction(self.index_name, id, body)
                actions += action

                if address.email not in all_emails:
                    i += 1
                all_emails.append(address.email)

        self.esClient.safe_put_bulk(actions)
        print("All mailman thread finished")

