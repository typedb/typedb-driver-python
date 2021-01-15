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


class Concept:

    def is_type(self):
        return False

    def is_thing_type(self):
        return False

    def is_entity_type(self):
        return False

    def is_attribute_type(self):
        return False

    def is_relation_type(self):
        return False

    def is_role_type(self):
        return False

    def is_thing(self):
        return False

    def is_entity(self):
        return False

    def is_attribute(self):
        return False

    def is_relation(self):
        return False

    def is_remote(self):
        return False


class RemoteConcept:

    def is_remote(self):
        return True

    def delete(self):
        pass

    def is_deleted(self):
        return False

    def is_type(self):
        return False

    def is_thing_type(self):
        return False

    def is_entity_type(self):
        return False

    def is_attribute_type(self):
        return False

    def is_relation_type(self):
        return False

    def is_role_type(self):
        return False

    def is_thing(self):
        return False

    def is_entity(self):
        return False

    def is_attribute(self):
        return False

    def is_relation(self):
        return False
