#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
from __future__ import print_function

from unittest import TestCase
from datetime import datetime

import os
import shutil
import six
import subprocess as sp
import tempfile
import zipfile


class ZipFile(zipfile.ZipFile):
    def _extract_member(self, member, targetpath, pwd):
        if not isinstance(member, zipfile.ZipInfo):
            member = self.getinfo(member)
        ret_val = super()._extract_member(member, targetpath, pwd)
        attr = member.external_attr >> 16
        os.chmod(ret_val, attr)
        return ret_val


class GraknServer(object):
    DISTRIBUTION_LOCATION = 'external/graknlabs_grakn_core/grakn-core-all-mac.zip'
    DISTRIBUTION_ROOT_DIR = 'grakn-core-all-mac'

    def __init__(self):
        self.__unpacked_dir = None

    def __enter__(self):
        if not self.__unpacked_dir:
            self._unpack()
        sp.check_call([
            'grakn', 'server', 'start'
        ], cwd=os.path.join(self.__unpacked_dir, GraknServer.DISTRIBUTION_ROOT_DIR))

    def __exit__(self, exc_type, exc_val, exc_tb):
        sp.check_call([
            'grakn', 'server', 'stop'
        ], cwd=os.path.join(self.__unpacked_dir, GraknServer.DISTRIBUTION_ROOT_DIR))
        shutil.rmtree(self.__unpacked_dir)

    def _unpack(self):
        self.__unpacked_dir = tempfile.mkdtemp(prefix='grakn')
        with ZipFile(GraknServer.DISTRIBUTION_LOCATION) as zf:
            zf.extractall(self.__unpacked_dir)


class test_Base(TestCase):
    """ Sets up DB for use in tests """

    @classmethod
    def setUpClass(cls):
        super(test_Base, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        super(test_Base, cls).tearDownClass()
