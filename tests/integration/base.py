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

import os
import shutil
import subprocess as sp
import tempfile
import sys
import tarfile


class GraknServer(object):
    DISTRIBUTION_LOCATION = sys.argv.pop()

    def __init__(self):
        self.__distribution_root_dir = None
        self.__unpacked_dir = None

    def __enter__(self):
        if not self.__unpacked_dir:
            self._unpack()
        sp.check_call([
            'grakn', 'server', 'start'
        ], cwd=os.path.join(self.__unpacked_dir, self.__distribution_root_dir))

    def __exit__(self, exc_type, exc_val, exc_tb):
        sp.check_call([
            'grakn', 'server', 'stop'
        ], cwd=os.path.join(self.__unpacked_dir, self.__distribution_root_dir))
        shutil.rmtree(self.__unpacked_dir)

    def _unpack(self):
        self.__unpacked_dir = tempfile.mkdtemp(prefix='grakn')
        with tarfile.open(GraknServer.DISTRIBUTION_LOCATION) as tf:
            tf.extractall(self.__unpacked_dir)
            self.__distribution_root_dir = os.path.commonpath(tf.getnames()[1:])


class test_Base(TestCase):
    """ Sets up DB for use in tests """

    @classmethod
    def setUpClass(cls):
        super(test_Base, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        super(test_Base, cls).tearDownClass()
