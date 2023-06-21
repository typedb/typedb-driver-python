from typedb.client import *


dbName = "datetime"


def main() -> None:
    # Create a client
    with TypeDB.core_client("localhost:1729") as client:
        with client.session("datetime", SessionType.DATA) as session:
            pass

        # Create a schema session
        with client.session(dbName, SessionType.SCHEMA) as session:
            with session.transaction(TransactionType.WRITE) as write_schema_transaction:
                # Only define a datetime attribute
                query = "define test_date sub attribute, value datetime;"
                write_schema_transaction.query().define(query)
                write_schema_transaction.commit()

        # Create a data session
        with client.session(dbName, SessionType.DATA) as session:
            with session.transaction(TransactionType.WRITE) as write_data_transaction:
                # Insert first of every month, at 00:00:00
                query = """insert
                $time_date_jan isa test_date;  $time_date_jan 2023-01-01T00:00:00;
                $time_date_feb isa test_date;  $time_date_feb 2023-02-01T00:00:00;
                $time_date_mar isa test_date;  $time_date_mar 2023-03-01T00:00:00;
                $time_date_apr isa test_date;  $time_date_apr 2023-04-01T00:00:00;
                $time_date_may isa test_date;  $time_date_may 2023-05-01T00:00:00;
                $time_date_jun isa test_date;  $time_date_jun 2023-06-01T00:00:00;
                $time_date_jul isa test_date;  $time_date_jul 2023-07-01T00:00:00;
                $time_date_aug isa test_date;  $time_date_aug 2023-08-01T00:00:00;
                $time_date_sep isa test_date;  $time_date_sep 2023-09-01T00:00:00;
                $time_date_oct isa test_date;  $time_date_oct 2023-10-01T00:00:00;
                $time_date_nov isa test_date;  $time_date_nov 2023-11-01T00:00:00;
                $time_date_dec isa test_date;  $time_date_dec 2023-12-01T00:00:00;
                """
                write_data_transaction.query().insert(query)
                write_data_transaction.commit()

            with session.transaction(TransactionType.READ) as read_data_transaction:
                # Match all date attributes and print them
                query = "match $date isa test_date; get $date;"
                answer = read_data_transaction.query().match(query)  # <- PROBLEM IS HERE
                dates = [ans.get("date") for ans in answer]
                for date in dates:
                    print(f"Retrieved date: {date.get_value()}")


if __name__ == "__main__":
    main()
