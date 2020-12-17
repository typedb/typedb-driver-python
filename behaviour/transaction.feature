Feature: Connection Transaction

  Background:
    Given connection does not have any database

  Scenario: one database, one session, one transaction to read
    When connection create database: grakn
    Given connection open session for database: grakn
    When session opens transaction of type: read
    Then session transaction is open: true
