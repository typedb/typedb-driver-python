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


from grakn.service.Session.Concept import BaseTypeMapping


class Concept(object):

    def __init__(self, grpc_concept):
        self.id = grpc_concept.id

    def as_remote(self, tx):
        from grakn.service.Session.Concept import ConceptFactory
        return ConceptFactory.create_remote_concept_base(tx._tx_service, self.id, BaseTypeMapping.name_to_grpc_base_type[object_to_name[type(self)]])

    def is_schema_concept(self):
        """ Check if this concept is a schema concept """
        return isinstance(self, SchemaConcept)

    is_schema_concept.__annotations__ = {'return': bool}

    def is_type(self):
        """ Check if this concept is a Type concept """
        return isinstance(self, Type)

    is_type.__annotations__ = {'return': bool}

    def is_thing(self):
        """ Check if this concept is a Thing concept """
        return isinstance(self, Thing)

    is_thing.__annotations__ = {'return': bool}

    def is_attribute_type(self):
        """ Check if this concept is an AttributeType concept """
        return isinstance(self, AttributeType)

    is_attribute_type.__annotations__ = {'return': bool}

    def is_entity_type(self):
        """ Check if this concept is an EntityType concept """
        return isinstance(self, EntityType)

    is_entity_type.__annotations__ = {'return': bool}

    def is_relation_type(self):
        """ Check if this concept is a RelationType concept """
        return isinstance(self, RelationType)

    is_relation_type.__annotations__ = {'return': bool}

    def is_role(self):
        """ Check if this concept is a Role """
        return isinstance(self, Role)

    is_role.__annotations__ = {'return': bool}

    def is_rule(self):
        """ Check if this concept is a Rule concept """
        return isinstance(self, Rule)

    is_rule.__annotations__ = {'return': bool}

    def is_attribute(self):
        """ Check if this concept is an Attribute concept """
        return isinstance(self, Attribute)

    is_attribute.__annotations__ = {'return': bool}

    def is_entity(self):
        """ Check if this concept is an Entity concept """
        return isinstance(self, Entity)

    is_entity.__annotations__ = {'return': bool}

    def is_relation(self):
        """ Check if this concept is a Relation concept """
        return isinstance(self, Relation)

    is_relation.__annotations__ = {'return': bool}


class SchemaConcept(Concept):

    def __init__(self, grpc_concept):
        super(SchemaConcept, self).__init__(grpc_concept)
        self._label = grpc_concept.label_res.label
        self._implicit = grpc_concept.isImplicit_res.implicit

    def label(self):
        """
        Get the label of this schema concept.
        """
        return self._label

    def is_implicit(self):
        """ Check if this schema concept is implicit """
        return self._implicit


class Type(SchemaConcept):
    pass


class EntityType(Type):
    pass


class AttributeType(Type):

    def __index__(self, grpc_concept):
        super(Type, self).__init__(grpc_concept)
        from grakn.service.Session.util import ResponseReader
        self._data_type = ResponseReader.ResponseReader.from_grpc_data_type_res(grpc_concept.dataType_res)

    def data_type(self):
        """ Get the DataType enum (grakn.DataType) corresponding to the type of this attribute """
        return self._data_type


class RelationType(Type):
    pass


class Rule(SchemaConcept):
    pass


class Role(SchemaConcept):
    pass


class Thing(Concept):

    def __init__(self, grpc_concept):
        super(Thing, self).__init__(grpc_concept)
        self._inferred = grpc_concept.inferred_res.inferred
        from grakn.service.Session.Concept import ConceptFactory
        self._type = ConceptFactory.create_local_concept(grpc_concept.type_res.type)

    def is_inferred(self):
        return self._inferred

    def type(self):
        return self._type


class Entity(Thing):
    pass


class Attribute(Thing):

    def __init__(self, grpc_concept):
        super(Attribute, self).__init__(grpc_concept)
        from grakn.service.Session.util import ResponseReader
        self._value = ResponseReader.ResponseReader.from_grpc_value_object(grpc_concept.value_res.value)

    def value(self):
        return self._value


class Relation(Thing):
    pass


name_to_object = {
    BaseTypeMapping.META_TYPE: Type,
    BaseTypeMapping.ENTITY_TYPE: EntityType,
    BaseTypeMapping.RELATION_TYPE: RelationType,
    BaseTypeMapping.ATTRIBUTE_TYPE: AttributeType,
    BaseTypeMapping.ROLE: Role,
    BaseTypeMapping.RULE: Rule,
    BaseTypeMapping.ENTITY: Entity,
    BaseTypeMapping.RELATION: Relation,
    BaseTypeMapping.ATTRIBUTE: Attribute
}

object_to_name = dict(zip(name_to_object.values(), name_to_object.keys()))
