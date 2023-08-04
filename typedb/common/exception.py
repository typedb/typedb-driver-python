#
# Copyright (C) 2022 Vaticle
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

from __future__ import annotations
from typing import Union, Any


class TypeDBClientException(Exception):

    def __init__(self, msg: Union[ErrorMessage, str], cause: BaseException = None, params: Any = None):
        if isinstance(msg, str):
            self.message = msg
            self.error_message = None
        else:
            self.message = msg.message(params)
            self.error_message = msg

        self.__cause__ = cause
        super(TypeDBClientException, self).__init__(self.message)

    @staticmethod
    def of(error_message: ErrorMessage, params: Any = None):
        return TypeDBClientException(msg=error_message, cause=None, params=params)


class ErrorMessage:

    def __init__(self, code_prefix: str, code_number: int, message_prefix: str, message_body: str):
        self._code_prefix = code_prefix
        self._code_number = code_number
        self._message = message_prefix + ": " + message_body

    def code(self) -> str:
        return self._code_prefix + str(self._code_number).zfill(2)

    def message(self, params: Any) -> str:
        return self._message % params if params else self._message

    def __str__(self):
        return "[%s] %s" % (self.code(), self._message)


class ClientErrorMessage(ErrorMessage):

    def __init__(self, code: int, message: str):
        super(ClientErrorMessage, self).__init__(code_prefix="CLI", code_number=code, message_prefix="Client Error", message_body=message)


RPC_METHOD_UNAVAILABLE = ClientErrorMessage(1, "The server does not support this method, please check "
                                               "the client-server compatibility:\n'%s'.")
CLIENT_NOT_OPEN = ClientErrorMessage(2, "The client is not open.")
SESSION_CLOSED = ClientErrorMessage(3, "The session has been closed and no further operation is allowed.")
TRANSACTION_CLOSED = ClientErrorMessage(4, "The transaction has been closed and no further operation is allowed.")
TRANSACTION_CLOSED_WITH_ERRORS = ClientErrorMessage(5, "The transaction has been closed with error(s):\n%s.")
UNABLE_TO_CONNECT = ClientErrorMessage(6, "Unable to connect to TypeDB server.")
POSITIVE_VALUE_REQUIRED = ClientErrorMessage(7, "Value cannot be less than 1, was: '%d'.")
MISSING_DB_NAME = ClientErrorMessage(8, "Database name cannot be empty.")
DB_DOES_NOT_EXIST = ClientErrorMessage(9, "The database '%s' does not exist.")
MISSING_RESPONSE = ClientErrorMessage(10, "Unexpected empty response for request ID '%s'.")
UNKNOWN_REQUEST_ID = ClientErrorMessage(11, "Received a response with unknown request id '%s':\n%s")
CLUSTER_NO_PRIMARY_REPLICA_YET = ClientErrorMessage(12, "No replica has been marked as the primary replica for "
                                                        "latest known term '%d'.")
CLUSTER_UNABLE_TO_CONNECT = ClientErrorMessage(13, "Unable to connect to TypeDB Cluster. Attempted connecting to "
                                                   "the cluster members, but none are available: '%s'.")
CLUSTER_REPLICA_NOT_PRIMARY = ClientErrorMessage(14, "The replica is not the primary replica.")
CLUSTER_ALL_NODES_FAILED = ClientErrorMessage(15, "Attempted connecting to all cluster members, but the following "
                                                  "errors occurred: \n%s")
CLUSTER_USER_DOES_NOT_EXIST = ClientErrorMessage(16, "The user '%s' does not exist.")
CLUSTER_TOKEN_CREDENTIAL_INVALID = ClientErrorMessage(17, "Invalid token credential.")
CLUSTER_INVALID_ROOT_CA_PATH = ClientErrorMessage(18, "The provided Root CA path '%s' does not exist.")
CLUSTER_INCONSISTENT_CREDENTIAL = ClientErrorMessage(21, "TLS disabled but the Root CA path provided.")
CLUSTER_CLIENT_CALLED_WITH_STRING = ClientErrorMessage(19, "The first argument of TypeDBClient.cluster() must be "
                                                           "a List of server addresses to connect to. It was called "
                                                           "with a string, not a List, which is not allowed.")
DATABASE_DELETED = ClientErrorMessage(20, "The database '%s' has been deleted and no further operation is allowed.")


class ConceptErrorMessage(ErrorMessage):

    def __init__(self, code: int, message: str):
        super(ConceptErrorMessage, self).__init__(code_prefix="CON", code_number=code,
                                                  message_prefix="Concept Error", message_body=message)


INVALID_CONCEPT_CASTING = ConceptErrorMessage(1, "Invalid concept conversion from '%s' to '%s'.")
MISSING_TRANSACTION = ConceptErrorMessage(2, "Transaction cannot be null.")
MISSING_IID = ConceptErrorMessage(3, "IID cannot be null or empty.")
MISSING_LABEL = ConceptErrorMessage(4, "Label cannot be null or empty.")
MISSING_VARIABLE = ConceptErrorMessage(14, "Variable name cannot be null or empty.")
BAD_ANNOTATION = ConceptErrorMessage(5, "The annotation '%s' was not recognised.")
BAD_ENCODING = ConceptErrorMessage(6, "The encoding '%s' was not recognised.")
BAD_VALUE_TYPE = ConceptErrorMessage(7, "The value type '%s' was not recognised.")
BAD_ATTRIBUTE_VALUE = ConceptErrorMessage(8, "The attribute value '%s' was not recognised.")
VALUE_HAS_NO_REMOTE = ConceptErrorMessage(9, "A 'value' has no remote concept.")
NONEXISTENT_EXPLAINABLE_CONCEPT = ConceptErrorMessage(10, "The concept identified by '%s' is not explainable.")
NONEXISTENT_EXPLAINABLE_OWNERSHIP = ConceptErrorMessage(11, "The ownership by owner '%s' of attribute '%s' "
                                                            "is not explainable.")
GET_HAS_WITH_MULTIPLE_FILTERS = ConceptErrorMessage(12, "Only one filter can be applied at a time to get_has. "
                                                        "The possible filters are: [attribute_type, attribute_types, "
                                                        "annotations]")
UNSUPPORTED_TIMEZONE_INFORMATION = ConceptErrorMessage(13, "A date-time attribute cannot accept timezone aware "
                                                           "datetime objects.")
UNRECOGNISED_ANNOTATION = ConceptErrorMessage(14, "The annotation '%s' is not recognised.")


class QueryErrorMessage(ErrorMessage):

    def __init__(self, code: int, message: str):
        super(QueryErrorMessage, self).__init__(code_prefix="QRY", code_number=code,
                                                message_prefix="Query Error", message_body=message)


VARIABLE_DOES_NOT_EXIST = QueryErrorMessage(1, "The variable '%s' does not exist.")
NO_EXPLANATION = QueryErrorMessage(2, "No explanation was found.")
BAD_ANSWER_TYPE = QueryErrorMessage(3, "The answer type '%s' was not recognised.")
MISSING_ANSWER = QueryErrorMessage(4, "The required field 'answer' of type '%s' was not set.")
MISSING_QUERY = QueryErrorMessage(5, "Query cannot be null or empty.")


class InternalErrorMessage(ErrorMessage):

    def __init__(self, code: int, message: str):
        super(InternalErrorMessage, self).__init__(code_prefix="PIN", code_number=code,
                                                   message_prefix="Python Internal Error", message_body=message)


UNEXPECTED_NATIVE_VALUE = InternalErrorMessage(1, "Unexpected native value encountered!")
ILLEGAL_STATE = InternalErrorMessage(2, "Illegal state has been reached!")
# ILLEGAL_ARGUMENT = InternalErrorMessage(3, "Illegal argument provided: '%s'")
ILLEGAL_CAST = InternalErrorMessage(3, "Illegal casting operation to '%s'.")
NULL_NATIVE_VALUE = InternalErrorMessage(4, "Unhandled null pointer to a native object encountered!")


class TypeDBException(Exception):

    def __init__(self, code: str, message: str):
        super().__init__(code, message)
        self.code = code
        self.message = message
