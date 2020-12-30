#
# Copyright (C) 2020 Grakn Labs
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

#noinspection CucumberUndefinedStep
Feature: Connection Transaction

  Background:
    Given connection has been opened
    Given connection does not have any database

  Scenario: one database, one session, one transaction to read
    When connection create database: grakn
    Given connection open session for database: grakn
    When session opens transaction of type: read
    Then session transaction is null: false
    Then session transaction is open: true
    Then session transaction has type: read

  Scenario: one database, one session, one transaction to write
    When connection create database: grakn
    Given connection open session for database: grakn
    When session opens transaction of type: write
    Then session transaction is null: false
    Then session transaction is open: true
    Then session transaction has type: write

  Scenario: one database, one session, one committed write transaction is closed
    When connection create database: grakn
    Given connection open session for database: grakn
    When session opens transaction of type: write
    Then session transaction commits
    Then session transaction commits; throws exception

  Scenario: one database, one session, re-committing transaction throws
    When connection create database: grakn
    Given connection open session for database: grakn
    When for each session, open transaction of type: write
    Then for each session, transaction commits
    Then for each session, transaction commits; throws exception

  Scenario: one database, one session, transaction close is idempotent
    When connection create database: grakn
    Given connection open session for database: grakn
    When for each session, open transaction of type: write
    Then for each session, transaction closes
    Then for each session, transaction is open: false
    Then for each session, transaction closes
    Then for each session, transaction is open: false

  @ignore-grakn-core
  Scenario: one database, one session, many transactions to read
    When connection create database: grakn
    Given connection open session for database: grakn
    When for each session, open transactions of type:
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
    Then for each session, transactions are null: false
    Then for each session, transactions are open: true
    Then for each session, transactions have type:
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |

  @ignore-grakn-core
  Scenario: one database, one session, many transactions to write
    When connection create database: grakn
    Given connection open session for database: grakn
    When for each session, open transactions of type:
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
    Then for each session, transactions are null: false
    Then for each session, transactions are open: true
    Then for each session, transactions have type:
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |

  @ignore-grakn-core
  Scenario: one database, one session, many transactions to read and write
    When connection create database: grakn
    Given connection open session for database: grakn
    When for each session, open transactions of type:
      | read  |
      | write |
      | read  |
      | write |
      | read  |
      | write |
      | read  |
      | write |
      | read  |
      | write |
      | read  |
      | write |
    Then for each session, transactions are null: false
    Then for each session, transactions are open: true
    Then for each session, transactions have type:
      | read  |
      | write |
      | read  |
      | write |
      | read  |
      | write |
      | read  |
      | write |
      | read  |
      | write |
      | read  |
      | write |

  Scenario: one database, one session, many transactions in parallel to read
    When connection create database: grakn
    Given connection open session for database: grakn
    When for each session, open transactions in parallel of type:
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
    Then for each session, transactions in parallel are null: false
    Then for each session, transactions in parallel are open: true
    Then for each session, transactions in parallel have type:
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |

  Scenario: one database, one session, many transactions in parallel to write
    When connection create database: grakn
    Given connection open session for database: grakn
    When for each session, open transactions in parallel of type:
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
    Then for each session, transactions in parallel are null: false
    Then for each session, transactions in parallel are open: true
    Then for each session, transactions in parallel have type:
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |

  Scenario: one database, one session, many transactions in parallel to read and write
    When connection create database: grakn
    Given connection open session for database: grakn
    When for each session, open transactions in parallel of type:
      | read  |
      | write |
      | read  |
      | write |
      | read  |
      | write |
      | read  |
      | write |
      | read  |
      | write |
      | read  |
      | write |
    Then for each session, transactions in parallel are null: false
    Then for each session, transactions in parallel are open: true
    Then for each session, transactions in parallel have type:
      | read  |
      | write |
      | read  |
      | write |
      | read  |
      | write |
      | read  |
      | write |
      | read  |
      | write |
      | read  |
      | write |

  Scenario: one database, many sessions, one transaction to read
    When connection create database: grakn
    Given connection open sessions for database:
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
    When for each session, open transaction of type: read
    Then for each session, transaction is null: false
    Then for each session, transaction is open: true
    Then for each session, transaction has type: read

  Scenario: one database, many sessions, one transaction to write
    When connection create database: grakn
    Given connection open sessions for database:
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
    When for each session, open transaction of type: write
    Then for each session, transaction is null: false
    Then for each session, transaction is open: true
    Then for each session, transaction has type: write

  @ignore-grakn-core
  Scenario: one database, many sessions, many transactions to read
    When connection create database: grakn
    Given connection open sessions for database:
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
    When for each session, open transactions of type:
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
    Then for each session, transactions are null: false
    Then for each session, transactions are open: true
    Then for each session, transactions have type:
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |

  @ignore-grakn-core
  Scenario: one database, many sessions, many transactions to write
    When connection create database: grakn
    Given connection open sessions for database:
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
    When for each session, open transactions of type:
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
    Then for each session, transactions are null: false
    Then for each session, transactions are open: true
    Then for each session, transactions have type:
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |

  @ignore-grakn-core
  Scenario: one database, many sessions, many transactions to read and write
    When connection create database: grakn
    Given connection open sessions for database:
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
    When for each session, open transactions of type:
      | read  |
      | write |
      | read  |
      | write |
      | read  |
      | write |
      | read  |
      | write |
      | read  |
      | write |
      | read  |
      | write |
    Then for each session, transactions are null: false
    Then for each session, transactions are open: true
    Then for each session, transactions have type:
      | read  |
      | write |
      | read  |
      | write |
      | read  |
      | write |
      | read  |
      | write |
      | read  |
      | write |
      | read  |
      | write |

  Scenario: one database, many sessions, many transactions in parallel to read
    When connection create database: grakn
    Given connection open sessions for database:
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
    When for each session, open transactions in parallel of type:
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
    Then for each session, transactions in parallel are null: false
    Then for each session, transactions in parallel are open: true
    Then for each session, transactions in parallel have type:
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |
      | read |

  Scenario: one database, many sessions, many transactions in parallel to write
    When connection create database: grakn
    Given connection open sessions for database:
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
    When for each session, open transactions in parallel of type:
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
    Then for each session, transactions in parallel are null: false
    Then for each session, transactions in parallel are open: true
    Then for each session, transactions in parallel have type:
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |
      | write |

  Scenario: one database, many sessions, many transactions in parallel to read and write
    When connection create database: grakn
    Given connection open sessions for database:
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
      | grakn |
    When for each session, open transactions in parallel of type:
      | read  |
      | write |
      | read  |
      | write |
      | read  |
      | write |
      | read  |
      | write |
      | read  |
      | write |
      | read  |
      | write |
    Then for each session, transactions in parallel are null: false
    Then for each session, transactions in parallel are open: true
    Then for each session, transactions in parallel have type:
      | read  |
      | write |
      | read  |
      | write |
      | read  |
      | write |
      | read  |
      | write |
      | read  |
      | write |
      | read  |
      | write |

#  Scenario: one database, many sessions in parallel, one transactions to read
#
#  Scenario: one database, many sessions in parallel, one transactions to write
#
#  Scenario: one database, many sessions in parallel, many transactions to read
#
#  Scenario: one database, many sessions in parallel, many transactions to write
#
#  Scenario: one database, many sessions in parallel, many transactions in parallel to read
#
#  Scenario: one database, many sessions in parallel, many transactions in parallel to write


  Scenario: write in a read transaction throws
    When connection create database: grakn
    Given connection open schema session for database: grakn
    When session opens transaction of type: read
    Then graql define
      """
      define person sub entity;
      """
    Then transaction commits; throws exception containing "write transactions can be committed"
