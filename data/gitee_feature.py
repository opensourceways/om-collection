#  Copyright (c) 2023.
#  Lorem ipsum dolor sit amet, consectetur adipiscing elit.
#  Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
#  Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
#  Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
#  Vestibulum commodo. Ut rhoncus gravida arcu.
import base64
import os
import re
import time

from data.common import ESClient


class giteeFeature(object):

    def __init__(self, config=None):
        self.config = config
        self.orgs = config.get('orgs')
        self.url = config.get('es_url')
        self.authorization = config.get('authorization')
        self.index_name = config.get('index_name')
        self.index_name_gitee = config.get('index_name_gitee')
        self.gitee_token = config.get('gitee_token')
        self.esClient = ESClient(config)
        self.username = config.get('username')
        self.password = config.get('password')

        self.repo_base_path = config.get('repo_base_path')
        self.org_name = config.get('org_name')
        self.repo_name = config.get('repo_name')
        self.local_path = ''
        self.code_path = ''

    def run(self, from_time):
        curr_time = time.strftime('%Y-%m-%dT%H:%M:%S+08:00', time.localtime())
        print(f'本次更新开始时间: {curr_time}')
        # 本地仓库地址
        self.local_path = os.path.join(self.repo_base_path, self.org_name, self.repo_name)
        self.code_path = os.path.join(self.local_path, self.repo_name)
        # 更新本地仓库
        self.download_gitee_repo()
        # # 获取所有版本
        versions = self.get_versions()
        for version in versions:
            feature_file = self.get_file(version = version)
            if os.path.exists(feature_file):
                self.parse_file(feature_file, version)
        curr_time = time.strftime('%Y-%m-%dT%H:%M:%S+08:00', time.localtime())
        print(f'本次更新结束时间: {curr_time}')

    def download_gitee_repo(self):
        if not os.path.exists(self.local_path):
            os.makedirs(self.local_path)
        user_name = base64.b64decode(self.username).decode()
        pass_word = base64.b64decode(self.password).decode()
        clone_url = 'https://%s:%s@gitee.com/%s/%s' % (user_name, pass_word, self.org_name, self.repo_name)

        # 本地仓库存在则执行git pull,不存在则执行git clone
        self.removeGitLockFile()
        if os.path.exists(self.code_path):
            cmd_pull = 'cd %s;git checkout .;git pull' % self.code_path
            os.system(cmd_pull)
        else:
            if clone_url is None:
                return
            cmd_clone = 'cd %s;git clone %s' % (self.local_path, clone_url + '.git')
            os.system(cmd_clone)

    # 删除git lock
    def removeGitLockFile(self):
        lock_file = self.code_path + '/.git/index.lock'
        if os.path.exists(lock_file):
            os.remove(lock_file)

    def get_versions(self):
        version_folder = self.code_path
        versions = []
        for ver in os.listdir(version_folder):
            # 判断文件夹名称是否是openEuler版本名称
            if os.path.isdir(os.path.join(version_folder, ver)) and ver.startswith("openEuler"):
                versions.append(ver)
        return versions

    def get_file(self, version):
        # 不同版本的特性文件名称不同
        feature_file = os.path.join(self.code_path, version, "release-plan.md")
        if os.path.exists(feature_file):
            return feature_file
        feature_file = os.path.join(self.code_path, version, "releaseplan.md")
        if os.path.exists(feature_file):
            return feature_file
        return ""

    def parse_file(self, feature_file, version):
        with open(feature_file, 'r', encoding="UTF-8") as f1:
            data = f1.readlines()
        last_row = len(data)
        # 变量first_row为版本特性标题(比如: no|feature|status|owner|sig)行号
        first_row = -1
        for i in range(last_row):
            line = data[i].lower()
            if 'no' in line and 'status' in line and 'owner' in line:
                first_row = i
                break
        if first_row == -1:
            return
        for i in range(first_row + 2, last_row):
            line = data[i]
            self.parse_line(line, version)

    def get_issue_url(self, origin):
        first_index = origin.find('https:')
        last_index = origin.find(')')
        url = origin[first_index : last_index]
        if '?from=' in url:
            issue_url = url.split('?')[0]
        else:
            issue_url = url
        return issue_url

    def get_feature_title(self, origin):
        first_index = origin.find('[')
        last_index = origin.find(']')
        return origin[first_index + 1 : last_index]

    def get_owner(self, owner):
        if len(owner) == 0:
            return []
        # 若owner字符串格式为：[@owner1](url1)[@owner2](url2)，从中提取出owner1,owner2，并以列表形式返回
        # 若owner字符串格式为：owner1，则直接返回列表
        owner = owner.replace('@', '')
        owners = re.findall('\[.*?\]', owner)
        # owners以列表展示，而不是以字符串展示
        if len(owners) != 0:
            # 去除变量owners[i]中的中括号字符，即:[]
            for i in range(len(owners)):
                owners[i] = owners[i][1: len(owners[i]) - 1]
        else:
            # 若owner字符串格式为：owner1，则直接返回列表
            owners = [owner]
        return owners

    def parse_line(self, line, version):
        items = line.split('|')
        if (len(items)) < 6:
            return
        no_idx = 1
        feature_idx = 2
        status_idx = 3
        sig_name_idx = 4
        owner_idx = 5
        no = items[no_idx]
        feature = items[feature_idx]
        status = items[status_idx].strip()
        sig_names = items[sig_name_idx].strip()
        owner = items[owner_idx].strip()
        base_label = version
        # issue_url有可能在变量no里，也有可能在变量feature里
        issue_url = ''
        feature_title = ''
        if 'https' in no:
            issue_url = self.get_issue_url(origin = no)
        if 'https' in feature:
            issue_url = self.get_issue_url(origin = feature)
            feature_title = self.get_feature_title(origin = feature)
        else:
            feature_title = feature
        # 处理owner,返回数据格式为list
        new_owner = self.get_owner(owner)
        # 从giteeall数据库读取
        try:
            user_login, tag_user_company, sig_name_from_index = self.esClient.get_data_from_index(issue_url)
        except Exception:
            print('连接es数据库失败')
            return
        # 如果feature中没有owner，则owner为user_login
        if len(new_owner) == 0:
            new_owner.append(user_login.strip())
        # 如果feature中没有sig_names,则sig_names为sig_name_from_index
        # sig_names格式为列表
        if len(sig_names) == 0:
            sig_names = sig_name_from_index
        else:
            sig_names = [sig_names]
        is_gitee_feature = 1
        is_enterprise_issue = 0
        # 判断issue_url是否是企业issue
        if len(issue_url) != 0 and 'e.gitee' in issue_url:
            is_enterprise_issue = 1
        update_at = time.strftime('%Y-%m-%dT%H:%M:%S+08:00', time.localtime())
        # 组装数据
        action = {
            "feature_title": feature_title,
            "base_label": base_label,
            "owner": new_owner,
            "status": status,
            "user_login": user_login,
            "tag_user_company": tag_user_company,
            "sig_names": sig_names,
            "is_gitee_feature": is_gitee_feature,
            "issue_url": issue_url,
            "is_enterprise_issue": is_enterprise_issue,
            "update_at": update_at
        }
        # 获取唯一id
        index_id = self.get_id(issue_url, base_label)
        # 若id不为空字符串，则写入数据库
        if len(index_id) != 0:
            # 只统计有issue的feature
            try:
                self.esClient.update_feature_index(action = action, index_id = index_id)
            except Exception:
                print(f'无法将数据更新到数据库： {index_id}')

    def get_id(self, issue_url, base_label):
        # 若issue_url格式：https://website.com/org/repo/issues/AAAAA，则提取org和repo及issue编号，返回唯一id字符串
        # 若issue_url为空字符串，则返回空字符串
        issue_url = issue_url.replace('https://', '')
        items = issue_url.split('/')
        if len(items) < 2:
            return ''
        org_index = 0
        repo_index = 1
        org = items[org_index]
        repo = items[repo_index]
        index_id = 'feature_version_' + base_label+ '_org_' + org + '_repo_' + repo + '_issue_' + issue_url[-6:]
        return index_id