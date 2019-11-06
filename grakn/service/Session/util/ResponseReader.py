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

import datetime
import six
from grakn.service.Session.util import enums
from grakn.service.Session.Concept import ConceptFactory
from grakn.exception.GraknError import GraknError 


class ResponseReader(object):
    
    @staticmethod
    def query(tx_service, grpc_query_iter):
        iterator_id = grpc_query_iter.id
        return ResponseIterator(tx_service,
                                iterator_id,
                                lambda tx_serv, iterate_res: AnswerConverter.convert(tx_serv, iterate_res.query_iter_res.answer))


    @staticmethod
    def get_concept(tx_service, grpc_get_schema_concept):
        which_one = grpc_get_schema_concept.WhichOneof("res")
        if which_one == "concept":
            grpc_concept = grpc_get_schema_concept.concept
            return ConceptFactory.create_concept(tx_service, grpc_concept)
        elif which_one == "null":
            return None
        else:
            raise GraknError("Unknown get_concept response: {0}".format(which_one))

    @staticmethod
    def get_schema_concept(tx_service, grpc_get_concept):
        which_one = grpc_get_concept.WhichOneof("res")
        if which_one == "schemaConcept":
            grpc_concept = grpc_get_concept.schemaConcept
            return ConceptFactory.create_concept(tx_service, grpc_concept)
        elif which_one == "null":
            return None
        else:
            raise GraknError("Unknown get_schema_concept response: {0}".format(which_one))

    @staticmethod
    def get_attributes_by_value(tx_service, grpc_get_attrs_iter):
        iterator_id = grpc_get_attrs_iter.id
        return ResponseIterator(tx_service,
                                iterator_id,
                                lambda tx_serv, iterate_res: ConceptFactory.create_concept(tx_serv, iterate_res.getAttributes_iter_res.attribute))

    @staticmethod
    def put_entity_type(tx_service, grpc_put_entity_type):
        return ConceptFactory.create_concept(tx_service, grpc_put_entity_type.entityType) 

    @staticmethod
    def put_relation_type(tx_service, grpc_put_relation_type):
        return ConceptFactory.create_concept(tx_service, grpc_put_relation_type.relationType)

    @staticmethod
    def put_attribute_type(tx_service, grpc_put_attribute_type):
        return ConceptFactory.create_concept(tx_service, grpc_put_attribute_type.attributeType)

    @staticmethod
    def put_role(tx_service, grpc_put_role):
        return ConceptFactory.create_concept(tx_service, grpc_put_role.role)

    @staticmethod
    def put_rule(tx_service, grpc_put_rule):
        return ConceptFactory.create_concept(tx_service, grpc_put_rule.rule)

    @staticmethod
    def from_grpc_value_object(grpc_value_object):
        whichone = grpc_value_object.WhichOneof('value')
        # check the one is in the known datatypes
        known_datatypes = [e.name.lower() for e in enums.DataType]
        if whichone.lower() not in known_datatypes:
            raise GraknError("Unknown value object value key: {0}, not in {1}".format(whichone, known_datatypes))
        if whichone == 'string':
            return grpc_value_object.string
        elif whichone == 'boolean':
            return grpc_value_object.boolean
        elif whichone == 'integer':
            return grpc_value_object.integer
        elif whichone == 'long':
            return grpc_value_object.long
        elif whichone == 'float':
            return grpc_value_object.float
        elif whichone == 'double':
            return grpc_value_object.double
        elif whichone == 'date':
            epoch_ms_utc = grpc_value_object.date
            local_datetime_utc = datetime.datetime.fromtimestamp(float(epoch_ms_utc)/1000.)
            return local_datetime_utc
        else:
            raise GraknError("Unknown datatype in enum but not handled in from_grpc_value_object")
        

    # --- concept method helpers ---

    @staticmethod
    def iter_res_to_iterator(tx_service, iterator_id, next_iteration_handler):
        return ResponseIterator(tx_service, iterator_id, next_iteration_handler)

    @staticmethod
    def create_explanation(tx_service, grpc_explanation_res):
        """ Convert gRPC explanation response to explanation object """
        grpc_list_of_concept_maps = grpc_explanation_res.explanation
        native_list_of_concept_maps = []
        for grpc_concept_map in grpc_list_of_concept_maps:
            native_list_of_concept_maps.append(AnswerConverter._create_concept_map(tx_service, grpc_concept_map))
        return Explanation(native_list_of_concept_maps)

class Explanation(object):

    def __init__(self, list_of_concept_maps):
        self._concept_maps_list = list_of_concept_maps

    def get_answers(self):
        """ Return answers this explanation is dependent on"""
        # note that concept_maps are subtypes of Answer
        return self._concept_maps_list


# ----- Different types of answers -----

class AnswerGroup(object):

    def __init__(self, owner_concept, answer_list):
        self._owner_concept = owner_concept
        self._answer_list = answer_list

    def get(self):
        return self

    def owner(self):
        return self._owner_concept

    def answers(self):
        return self._answer_list


class ConceptMap(object):

    def __init__(self, concept_map, query_pattern, has_explanation, tx_service):
        self._concept_map = concept_map
        self._has_explanation = has_explanation
        self._query_pattern = query_pattern
        self._tx_service = tx_service

    def get(self, var=None):
        """ Get the indicated variable's Concept from the map or this ConceptMap """
        if var is None:
            return self
        else:
            if var not in self._concept_map:
                # TODO specialize exception
                raise GraknError("Variable {0} is not in the ConceptMap".format(var))
            return self._concept_map[var]

    def query_pattern(self):
        return self._query_pattern

    def has_explanation(self):
        return self._has_explanation

    def explanation(self):
        if self._has_explanation:
            return self._tx_service.explanation(self)
        else:
            raise GraknError("Explanation not available on concept map: " + str(self))

    def map(self):
        """ Get the map from Variable (str) to Concept objects """
        return self._concept_map

    def vars(self):
        """ Get a set of vars in the map """
        return set(self._concept_map.keys())

    def contains_var(self, var):
        """ Check whether the map contains the var """
        return var in self._concept_map

    def is_empty(self):
        """ Check if the variable map is empty """
        return len(self._concept_map) == 0


class ConceptList(object):

    def __init__(self, concept_id_list):
        self._concept_id_list = concept_id_list

    def list(self):
        """ Get the list of concept IDs """
        return self._concept_id_list


class ConceptSet(object):

    def __init__(self, concept_id_set):
        self._concept_id_set = concept_id_set
    __init__.__annotations__ = {'_concept_id_set': 'List[str]'}

    def set(self):
        """ Return the set of Concept IDs within this ConceptSet """
        return self._concept_id_set


class ConceptSetMeasure(ConceptSet):

    def __init__(self, concept_id_set, number):
        super(ConceptSetMeasure, self).__init__(concept_id_set)
        self._measurement = number
    __init__.__annotations__ = {'_measurement': float}

    def measurement(self):
        return self._measurement


class Value(object):

    def __init__(self, number):
        self._number = number
    __init__.__annotations__ = {'number': float}

    def number(self):
        """ Get as number (float or int) """
        return self._number


class Void(object):
    def __init__(self, message):
        self._message = message
    __init__.__annotations__ = {'message': str}

    def message(self):
        """ Get the message on this Void answer type """
        return self._message


class AnswerConverter(object):
    """ Static methods to convert answers into Answer objects """

    @staticmethod
    def convert(tx_service, grpc_answer):
        which_one = grpc_answer.WhichOneof('answer')

        if which_one == 'conceptMap':
            return AnswerConverter._create_concept_map(tx_service, grpc_answer.conceptMap)
        elif which_one == 'answerGroup':
            return AnswerConverter._create_answer_group(tx_service, grpc_answer.answerGroup)
        elif which_one == 'conceptList':
            return AnswerConverter._create_concept_list(tx_service, grpc_answer.conceptList)
        elif which_one == 'conceptSet':
            return AnswerConverter._create_concept_set(tx_service, grpc_answer.conceptSet)
        elif which_one == 'conceptSetMeasure':
            return AnswerConverter._create_concept_set_measure(tx_service, grpc_answer.conceptSetMeasure)
        elif which_one == 'value':
            return AnswerConverter._create_value(tx_service, grpc_answer.value)
        elif which_one == 'void':
            return AnswerConverter._create_void(tx_service, grpc_answer.void)
        else:
            raise GraknError('Unknown gRPC Answer.answer message type: {0}'.format(which_one))
   
    @staticmethod
    def _create_concept_map(tx_service, grpc_concept_map_msg):
        """ Create a Concept Dictionary from the grpc response """
        var_concept_map = grpc_concept_map_msg.map
        answer_map = {}
        for (variable, grpc_concept) in var_concept_map.items():
            answer_map[variable] = ConceptFactory.create_concept(tx_service, grpc_concept)

        query_pattern = grpc_concept_map_msg.pattern
        has_explanation = grpc_concept_map_msg.hasExplanation

        return ConceptMap(answer_map,  query_pattern, has_explanation, tx_service)

    @staticmethod
    def _create_answer_group(tx_service, grpc_answer_group):
        grpc_owner_concept = grpc_answer_group.owner
        owner_concept = ConceptFactory.create_concept(tx_service, grpc_owner_concept)
        grpc_answers = list(grpc_answer_group.answers)
        answer_list = [AnswerConverter.convert(tx_service, grpc_answer) for grpc_answer in grpc_answers]
        return AnswerGroup(owner_concept, answer_list)

    @staticmethod
    def _create_concept_list(tx_service, grpc_concept_list_msg):
        ids_list = list(grpc_concept_list_msg.list.ids)
        return ConceptList(ids_list)

    @staticmethod
    def _create_concept_set(tx_service, grpc_concept_set_msg):
        ids_set = set(grpc_concept_set_msg.set.ids)
        return ConceptSet(ids_set)

    @staticmethod
    def _create_concept_set_measure(tx_service, grpc_concept_set_measure):
        concept_ids = list(grpc_concept_set_measure.set.ids)
        number = grpc_concept_set_measure.measurement.value 
        return ConceptSetMeasure(concept_ids, AnswerConverter._number_string_to_native(number))

    @staticmethod
    def _create_value(tx_service, grpc_value_msg):
        number = grpc_value_msg.number.value 
        return Value(AnswerConverter._number_string_to_native(number))

    @staticmethod
    def _create_void(tx_service, grpc_void):
        """ Convert grpc Void message into an object """
        return Void(grpc_void.message)

    @staticmethod
    def _number_string_to_native(number):
        try:
            return int(number)
        except ValueError:
            return float(number)


class ResponseIterator(six.Iterator):
    """ Retrieves next value in the Grakn response iterator """

    def __init__(self, tx_service , iterator_id, iter_resp_converter):
        self._tx_service = tx_service  
        self.iterator_id = iterator_id
        self._iter_resp_converter = iter_resp_converter

    def __iter__(self):
        return self

    def __next__(self):
        # get next from server
        iter_response = self._tx_service.iterate(self.iterator_id)
        # print("Iterator response:")
        # print(iter_response)
        which_one = iter_response.WhichOneof("res")
        if which_one == 'done' and iter_response.done:
            raise StopIteration()
        else:
            return self._iter_resp_converter(self._tx_service, iter_response)

    def collect_concepts(self):
        """ Helper method to retrieve concepts from a query() method """
        concepts = []
        for answer in self:
            if not isinstance(answer, ConceptMap):
                raise GraknError("Only use .collect_concepts on ConceptMaps returned by query()")
            concepts.extend(answer.map().values()) # get concept map => concepts
        return concepts





