from typing import List

from grakn.rpc.transaction import TransactionType


def parse_bool(value: str) -> bool:
    return value == "true"


def parse_list(table) -> List[str]:
    return [table.headings[0]] + list(map(lambda row: row[0], table.rows))


def parse_transaction_type(value: str) -> TransactionType:
    return TransactionType.READ if value == "read" else TransactionType.WRITE

