class Concept(object):

    def is_type(self):
        return False

    def is_thing(self):
        return False

    def is_remote(self):
        return False


class RemoteConcept(object):

    def is_remote(self):
        return True

    def delete(self):
        pass

    def is_deleted(self):
        return False

    def is_type(self):
        return False

    def is_thing(self):
        return False
