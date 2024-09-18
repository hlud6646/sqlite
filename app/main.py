import sys

from dataclasses import dataclass

# import sqlparse - available if you need it!

database_file_path = sys.argv[1]
command = sys.argv[2]


def page_size(database_file_path):
    with open(database_file_path, "rb") as database_file:
        database_file.seek(16)
        return int.from_bytes(database_file.read(2), byteorder="big")


def page_type(database_file_path):
    with open(database_file_path, "rb") as database_file:
        database_file.seek(100)
        return hex(int.from_bytes(database_file.read(1), byteorder="big"))


def n_tables(database_file_path):
    with open(database_file_path, "rb") as database_file:
        # Skip the database header and navigate B-tree page header.
        database_file.seek(100 + 3)
        return int.from_bytes(database_file.read(2), byteorder="big")


if command == ".dbinfo":
    print(f"database page size: {page_size(database_file_path)}")
    print(f"number of tables: {n_tables(database_file_path)}")
elif command == "dev":
    print(f"dev: {page_type(database_file_path)}")
else:
    print(f"Invalid command: {command}")

