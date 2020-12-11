import enum


# This lives here to avoid circular imports.
class ValueType(enum.Enum):
    OBJECT = 0
    BOOLEAN = 1
    LONG = 2
    DOUBLE = 3
    STRING = 4
    DATETIME = 5
