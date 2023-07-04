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


import argparse
import logging
import sys

import collection
import collection.backend
import collection.backends.core

COLLECTION_USAGE_MSG = \
    """%(prog)s [-g] <backend> [<args>] | --help | --version | --list"""

COLLECTION_DESC_MSG = \
    """Send Sir Collection on a quest to retrieve and gather data from software
repositories.

Repositories are reached using specific backends. The most common backends
are:

    askbot           Fetch questions and answers from Askbot site
    bugzilla         Fetch bugs from a Bugzilla server
    bugzillarest     Fetch bugs from a Bugzilla server (>=5.0) using its REST API
    confluence       Fetch contents from a Confluence server
    discourse        Fetch posts from Discourse site
    dockerhub        Fetch repository data from Docker Hub site
    gerrit           Fetch reviews from a Gerrit server
    git              Fetch commits from Git
    github           Fetch issues, pull requests and repository information from GitHub
    gitlab           Fetch issues, merge requests from GitLab
    gitter           Fetch messages from a Gitter room
    googlehits       Fetch hits from Google API
    groupsio         Fetch messages from Groups.io
    hyperkitty       Fetch messages from a HyperKitty archiver
    jenkins          Fetch builds from a Jenkins server
    jira             Fetch issues from JIRA issue tracker
    launchpad        Fetch issues from Launchpad issue tracker
    mattermost       Fetch posts from a Mattermost server
    mbox             Fetch messages from MBox files
    mediawiki        Fetch pages and revisions from a MediaWiki site
    meetup           Fetch events from a Meetup group
    nntp             Fetch articles from a NNTP news group
    pagure           Fetch issues from Pagure
    phabricator      Fetch tasks from a Phabricator site
    pipermail        Fetch messages from a Pipermail archiver
    redmine          Fetch issues from a Redmine server
    rocketchat       Fetch messages from a Rocket.Chat channel
    rss              Fetch entries from a RSS feed server
    slack            Fetch messages from a Slack channel
    stackexchange    Fetch questions from StackExchange sites
    supybot          Fetch messages from Supybot log files
    telegram         Fetch messages from the Telegram server
    twitter          Fetch tweets from the Twitter Search API

optional arguments:
  -h, --help            show this help message and exit
  -v, --version         show version
  -g, --debug           set debug mode on
  -l, --list            show available backends
"""

COLLECTION_EPILOG_MSG = \
    """Run '%(prog)s <backend> --help' to get information about a specific backend."""

COLLECTION_VERSION_MSG = \
    """%(prog)s """ + collection.backends.core.__version__


# Logging formats
COLLECTION_LOG_FORMAT = "[%(asctime)s] - %(message)s"
COLLECTION_DEBUG_LOG_FORMAT = "[%(asctime)s - %(name)s - %(levelname)s] - %(message)s"


class ListBackends(argparse.Action):

    def __init__(self, option_strings, dest, **kwargs):
        self.backends = kwargs.get('backends', None)
        del kwargs['backends']
        super().__init__(option_strings, dest, nargs=0, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        backends = sorted(self.backends.keys())
        for backend in backends:
            sys.stdout.write(backend + '\n')
        parser.exit()


def main():
    _, COLLECTION_CMDS = collection.backend.find_backends(collection.backends)

    args = parse_args(COLLECTION_CMDS)

    if args.backend not in COLLECTION_CMDS:
        raise RuntimeError("Unknown backend %s" % args.backend)
    configure_logging(args.debug)

    logging.info("Sir Collection is on his quest.")
    klass = COLLECTION_CMDS[args.backend]
    cmd = klass(*args.backend_args, debug=args.debug)
    cmd.run()

    logging.info("Sir Collection completed his quest.")


def parse_args(collection_cmds):
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(usage=COLLECTION_USAGE_MSG,
                                     description=COLLECTION_DESC_MSG,
                                     epilog=COLLECTION_EPILOG_MSG,
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     add_help=False)

    parser.add_argument('-h', '--help', action='help',
                        help=argparse.SUPPRESS)
    parser.add_argument('-v', '--version', action='version',
                        version=COLLECTION_VERSION_MSG,
                        help=argparse.SUPPRESS)
    parser.add_argument('-g', '--debug', dest='debug',
                        action='store_true',
                        help=argparse.SUPPRESS)
    parser.add_argument('-l', '--list', backends=collection_cmds, action=ListBackends,
                        help=argparse.SUPPRESS)

    parser.add_argument('backend', help=argparse.SUPPRESS)
    parser.add_argument('backend_args', nargs=argparse.REMAINDER,
                        help=argparse.SUPPRESS)

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    return parser.parse_args()


def configure_logging(debug=False):
    """Configure Collection logging

    The function configures the log messages produced by Collection.
    By default, log messages are sent to stderr. Set the parameter
    `debug` to activate the debug mode.

    :param debug: set the debug mode
    """
    if not debug:
        logging.basicConfig(level=logging.INFO,
                            format=COLLECTION_LOG_FORMAT)
        logging.getLogger('requests').setLevel(logging.WARNING)
        logging.getLogger('urrlib3').setLevel(logging.WARNING)
    else:
        logging.basicConfig(level=logging.DEBUG,
                            format=COLLECTION_DEBUG_LOG_FORMAT)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        s = "\n\nReceived Ctrl-C or other break signal. Exiting.\n"
        sys.stderr.write(s)
        sys.exit(0)
