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

import unittest
from datetime import datetime

from grakn.client import GraknClient, SessionType, TransactionType, ValueType
from grakn.common.exception import GraknClientException
from tests.integration.base import test_base, GraknServer


class TestConcept(test_base):
    @classmethod
    def setUpClass(cls):
        super(TestConcept, cls).setUpClass()
        global client
        client = GraknClient()

    @classmethod
    def tearDownClass(cls):
        super(TestConcept, cls).tearDownClass()
        client.close()

    def setUp(self):
        if "grakn" in client.databases().all():
            client.databases().delete("grakn")
        client.databases().create("grakn")

    def test_get_supertypes(self):
        with client.session("grakn", SessionType.SCHEMA) as session:
            with session.transaction(TransactionType.WRITE) as tx:
                lion = tx.concepts().put_entity_type("lion")
                for lion_supertype in lion.as_remote(tx).get_supertypes():
                    print(str(lion_supertype) + " is a supertype of 'lion'")

    def test_streaming_operation_on_closed_tx(self):
        with client.session("grakn", SessionType.SCHEMA) as session:
            with session.transaction(TransactionType.WRITE) as tx:
                lion = tx.concepts().put_entity_type("lion")
                tx.close()
                try:
                    for _ in lion.as_remote(tx).get_supertypes():
                        self.fail()
                    self.fail()
                except GraknClientException:
                    pass

    def test_invalid_streaming_operation(self):
        with client.session("grakn", SessionType.SCHEMA) as session:
            with session.transaction(TransactionType.WRITE) as tx:
                lion = tx.concepts().put_entity_type("lion")
                lion._label = "lizard"
                try:
                    for _ in lion.as_remote(tx).get_supertypes():
                        self.fail()
                    self.fail()
                except GraknClientException:
                    pass

    def test_get_many_instances(self):
        with client.session("grakn", SessionType.SCHEMA) as session:
            with session.transaction(TransactionType.WRITE) as tx:
                goldfish_type = tx.concepts().put_entity_type("goldfish")
                tx.commit()
        with client.session("grakn", SessionType.DATA) as session:
            with session.transaction(TransactionType.WRITE) as tx:
                for _ in range(100):
                    goldfish_type.as_remote(tx).create()
                goldfish_count = sum(1 for _ in goldfish_type.as_remote(tx).get_instances())
                print("There are " + str(goldfish_count) + " goldfish.")

    def test_stone_lions(self):
        # SCHEMA OPERATIONS
        with client.session("grakn", SessionType.SCHEMA) as session:
            with session.transaction(TransactionType.WRITE) as tx:
                lion = tx.concepts().put_entity_type("lion")
                tx.commit()
                print("put_entity_type - SUCCESS")

            with session.transaction(TransactionType.WRITE) as tx:
                lionFamily = tx.concepts().put_relation_type("lion-family")
                lionFamily.as_remote(tx).set_relates("lion-cub")
                lionCub = next(lionFamily.as_remote(tx).get_relates())
                lion.as_remote(tx).set_plays(lionCub)
                tx.commit()
                print("put_relation_type / set_relates / set_plays - SUCCESS")

            with session.transaction(TransactionType.WRITE) as tx:
                maneSize = tx.concepts().put_attribute_type("mane-size", ValueType.LONG)
                lion.as_remote(tx).set_owns(maneSize)
                tx.commit()
                print("commit attribute type + owns - SUCCESS")

            with session.transaction(TransactionType.WRITE) as tx:
                stoneLion = tx.concepts().put_entity_type("stone-lion")
                stoneLion.as_remote(tx).set_supertype(lion)
                tx.commit()
                print("set supertype - SUCCESS")

            with session.transaction(TransactionType.READ) as tx:
                supertypeOfLion = lion.as_remote(tx).get_supertype()
                tx.close()
                print("get supertype - SUCCESS - the supertype of 'lion' is '" + supertypeOfLion.get_label() + "'")

            with session.transaction(TransactionType.READ) as tx:
                supertypesOfStoneLion = list(map(lambda x: x.get_label(), stoneLion.as_remote(tx).get_supertypes()))
                print("get supertypes - SUCCESS - the supertypes of 'stone-lion' are " + str(supertypesOfStoneLion))

            with session.transaction(TransactionType.READ) as tx:
                subtypesOfLion = list(map(lambda x: x.get_label(), lion.as_remote(tx).get_subtypes()))
                print("get subtypes - SUCCESS - the subtypes of 'lion' are " + str(subtypesOfLion))

            with session.transaction(TransactionType.WRITE) as tx:
                monkey = tx.concepts().put_entity_type("monkey")
                monkey.as_remote(tx).set_label("orangutan")
                newLabel = tx.concepts().get_entity_type("orangutan").get_label()
                tx.rollback()
                assert newLabel == "orangutan"
                print("set label - SUCCESS - 'monkey' has been renamed to '" + newLabel + "'.")

            with session.transaction(TransactionType.WRITE) as tx:
                whale = tx.concepts().put_entity_type("whale")
                whale.as_remote(tx).set_abstract()
                isAbstractAfterSet = whale.as_remote(tx).is_abstract()
                assert isAbstractAfterSet
                print("set abstract - SUCCESS - 'whale' " + ("is" if isAbstractAfterSet else "is not") + " abstract.")
                whale.as_remote(tx).unset_abstract()
                isAbstractAfterUnset = whale.as_remote(tx).is_abstract()
                assert not isAbstractAfterUnset
                tx.rollback()
                print("unset abstract - SUCCESS - 'whale' " + ("is still" if isAbstractAfterUnset else "is no longer") + " abstract.")

            with session.transaction(TransactionType.WRITE) as tx:
                parentship = tx.concepts().put_relation_type("parentship")
                parentship.as_remote(tx).set_relates("parent")
                fathership = tx.concepts().put_relation_type("fathership")
                fathership.as_remote(tx).set_supertype(parentship)
                fathership.as_remote(tx).set_relates("father", "parent")
                person = tx.concepts().put_entity_type("person")
                parent = parentship.as_remote(tx).get_relates("parent")
                person.as_remote(tx).set_plays(parent)
                man = tx.concepts().put_entity_type("man")
                man.as_remote(tx).set_supertype(person)
                father = fathership.as_remote(tx).get_relates("father")
                man.as_remote(tx).set_plays(father, parent)
                playingRoles = list(map(lambda role: role.get_scoped_label(), man.as_remote(tx).get_plays()))
                roleplayers = list(map(lambda player: player.get_label(), father.as_remote(tx).get_players()))
                tx.commit()
                assert "fathership:father" in playingRoles
                assert "man" in roleplayers
                print("get/set relates/plays/players, overriding a super-role - SUCCESS - 'man' plays " + str(playingRoles) + "; 'fathership:father' is played by " + str(roleplayers))

            with session.transaction(TransactionType.WRITE) as tx:
                email = tx.concepts().put_attribute_type("email", ValueType.STRING)
                email.as_remote(tx).set_abstract()
                workEmail = tx.concepts().put_attribute_type("work-email", ValueType.STRING)
                workEmail.as_remote(tx).set_supertype(email)
                age = tx.concepts().put_attribute_type("age", ValueType.LONG)
                assert age.is_long()
                person.as_remote(tx).set_abstract()
                person.as_remote(tx).set_owns(attribute_type=email, is_key=True)
                person.as_remote(tx).set_owns(attribute_type=age, is_key=False)
                lion.as_remote(tx).set_owns(attribute_type=age)
                customer = tx.concepts().put_entity_type("customer")
                customer.as_remote(tx).set_supertype(person)
                customer.as_remote(tx).set_owns(attribute_type=workEmail, is_key=True, overridden_type=email)
                ownedAttributes = list(map(lambda x: x.get_label(), customer.as_remote(tx).get_owns()))
                ownedKeys = list(map(lambda x: x.get_label(), customer.as_remote(tx).get_owns(keys_only=True)))
                ownedDateTimes = list(map(lambda x: x.get_label(), customer.as_remote(tx).get_owns(ValueType.DATETIME, keys_only=False)))
                tx.commit()
                assert len(ownedAttributes) == 2
                assert len(ownedKeys) == 1
                assert len(ownedDateTimes) == 0
                print("get/set owns, overriding a super-attribute - SUCCESS - 'customer' owns " + str(ownedAttributes) + ", "
                      "of which " + str(ownedKeys) + " are keys, and " + str(ownedDateTimes) + " are datetimes")

            with session.transaction(TransactionType.WRITE) as tx:
                person.as_remote(tx).unset_owns(age)
                person.as_remote(tx).unset_plays(parent)
                fathership.as_remote(tx).unset_relates("father")
                personOwns = list(map(lambda x: x.get_label(), person.as_remote(tx).get_owns()))
                personPlays = list(map(lambda x: x.get_label(), person.as_remote(tx).get_plays()))
                fathershipRelates = list(map(lambda x: x.get_label(), fathership.as_remote(tx).get_relates()))
                tx.rollback()
                assert "age" not in personOwns
                assert "parent" not in personPlays
                assert "father" not in fathershipRelates
                print("unset owns/plays/relates - SUCCESS - 'person' owns " + str(personOwns) + ", "
                "'person' plays " + str(personPlays) + ", 'fathership' relates " + str(fathershipRelates))

            with session.transaction(TransactionType.WRITE) as tx:
                password = tx.concepts().put_attribute_type("password", ValueType.STRING)
                shoeSize = tx.concepts().put_attribute_type("shoe-size", ValueType.LONG)
                volume = tx.concepts().put_attribute_type("volume", ValueType.DOUBLE)
                isAlive = tx.concepts().put_attribute_type("is-alive", ValueType.BOOLEAN)
                startDate = tx.concepts().put_attribute_type("start-date", ValueType.DATETIME)
                tx.commit()
                print("put all 5 attribute value types - SUCCESS - password is a " + password.get_value_type().name + ", shoe-size is a " + shoeSize.get_value_type().name + ", "
                      "volume is a " + volume.get_value_type().name + ", is-alive is a " + isAlive.get_value_type().name + " and start-date is a " + startDate.get_value_type().name)

            with session.transaction(TransactionType.WRITE) as tx:
                tx.logic().put_rule("septuagenarian-rule", "{$x isa person;}", "$x has age 70")
                tx.commit()
                print("put rule - SUCCESS")

        # DATA OPERATIONS
        with client.session("grakn", SessionType.DATA) as session:
            with session.transaction(TransactionType.WRITE) as tx:
                for _ in range(10): stoneLion.as_remote(tx).create()
                lions = list(lion.as_remote(tx).get_instances())
                firstLion = lions[0]
                isInferred = firstLion.as_remote(tx).is_inferred()
                lionType = firstLion.as_remote(tx).get_type()
                age42 = age.as_remote(tx).put(42)
                firstLion.as_remote(tx).set_has(age42)
                firstLionAttrs = list(map(lambda x: x.get_value(), firstLion.as_remote(tx).get_has()))
                assert len(firstLionAttrs) == 1
                assert firstLionAttrs[0] == 42
                firstLionAges = list(map(lambda x: x.get_value(), firstLion.as_remote(tx).get_has(age)))
                assert len(firstLionAges) == 1
                assert firstLionAges[0] == 42
                firstLionWorkEmails = list(map(lambda x: x.get_value(), firstLion.as_remote(tx).get_has(workEmail)))
                assert len(firstLionWorkEmails) == 0
                firstFamily = lionFamily.as_remote(tx).create()
                firstFamily.as_remote(tx).add_player(lionCub, firstLion)
                firstLionPlaying = list(map(lambda x: x.get_scoped_label(), firstLion.as_remote(tx).get_plays()))
                assert len(firstLionPlaying) == 1
                assert firstLionPlaying[0] == "lion-family:lion-cub"
                firstLionRelations = list(firstLion.as_remote(tx).get_relations())
                assert len(firstLionRelations) == 1
                firstLionFatherRelations = list(firstLion.as_remote(tx).get_relations([father]))
                assert len(firstLionFatherRelations) == 0
                tx.commit()
                assert len(lions) == 10
                assert not isInferred
                print("Thing methods - SUCCESS - There are " + str(len(lions)) + " lions.")
                assert lionType.get_label() == "stone-lion"
                print("getType - SUCCESS - After looking more closely, it turns out that there are " + str(len(lions)) + " stone lions.")

            with session.transaction(TransactionType.WRITE) as tx:
                firstLionFamily = next(lionFamily.as_remote(tx).get_instances())
                firstLion = next(firstLionFamily.as_remote(tx).get_players())
                firstLionFamily2 = next(firstLion.as_remote(tx).get_relations())
                assert firstLionFamily2
                players = list(firstLionFamily.as_remote(tx).get_players())
                assert len(players) == 1
                lionCubPlayers = list(firstLionFamily.as_remote(tx).get_players([lionCub]))
                assert len(lionCubPlayers) == 1
                playersByRoleType = firstLionFamily.as_remote(tx).get_players_by_role_type().items()
                (firstRole, firstPlayer) = next(iter(playersByRoleType))
                assert firstRole.get_scoped_label() == "lion-family:lion-cub"
                firstLionFamily.as_remote(tx).remove_player(lionCub, firstLion)
                lionFamilyCleanedUp = firstLionFamily.as_remote(tx).is_deleted()
                assert(lionFamilyCleanedUp)
                tx.rollback()
                print("Relation methods - SUCCESS")

            with session.transaction(TransactionType.WRITE) as tx:
                passwordAttr = password.as_remote(tx).put("rosebud")
                shoeSizeAttr = shoeSize.as_remote(tx).put(9)
                volumeAttr = volume.as_remote(tx).put(1.618)
                isAliveAttr = isAlive.as_remote(tx).put(bool("hopefully"))
                startDateAttr = startDate.as_remote(tx).put(datetime.now())
                tx.commit()
                print("put 5 different types of attributes - SUCCESS - password is " + passwordAttr.get_value() + ", shoe-size is " + str(shoeSizeAttr.get_value()) + ", "
                      "volume is " + str(volumeAttr.get_value()) + ", is-alive is " + str(isAliveAttr.get_value()) + " and start-date is " + str(startDateAttr.get_value()))


if __name__ == "__main__":
    with GraknServer():
        unittest.main(verbosity=2)
