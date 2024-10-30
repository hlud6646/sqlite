"""
Building SQLite.

What have I learned in this project:
 - SQLite file format;
 - Navigating binary files;
 - Bitwise operations;
"""

import sys

# import sqlparse - available if you need it!

database_file_path = sys.argv[1]
command = sys.argv[2]


# Refer to the SQLite file format at https://www.sqlite.org/fileformat.html
DATABASE_HEADER_BYTES = 100
PAGE_HEADER_BYTES = 8
LEAF_PAGE_HEADER_BYTES = 8
PAGE_SIZE_OFFSET = 16
CELL_COUNT_OFFSET = 3


def read_database_header(database_file_path):
    with open(database_file_path, "rb") as db_file:
        data = db_file.read(DATABASE_HEADER_BYTES)
        # Refer to '1.3. The Database Header' for the offsets/lengths.
        return {
            "magic_header_string": data[:16],
            "page_size": int.from_bytes(data[16:18]),
            "text_encoding": int.from_bytes(data[56:60]),
            # Extend this parsing as needed.
        }


def read_page(database_file_path, pagesize, offset):
    """Read the page at the given offset."""
    with open(database_file_path, "rb") as db_file:
        db_file.seek(offset)
        return db_file.read(pagesize)


def read_page_header(page, skip_db_header=False):
    # The first page in a database file contains the database header.
    # If you want to read the page header of this page, you need to read
    # past the database header first.
    if skip_db_header:
        page = page[DATABASE_HEADER_BYTES:]
    header = page[:8]
    return {
        "page_type": header[0],
        "n_cells": int.from_bytes(header[3:5]),
    }


def cell_pointer_offsets(page, n_cells, skip_db_header=False):
    if skip_db_header:
        page = page[DATABASE_HEADER_BYTES:]
    page = page[PAGE_HEADER_BYTES:]
    offsets = []
    for i in range(n_cells):
        offsets.append(int.from_bytes(page[i * 2 : (i + 1) * 2]))
    return offsets


# TODO: Test this.
def varint(data):
    """
    Decode a single varint from bytes and return it and the rest
    of the data."""
    result = 0
    shift = 0

    for i, byte in enumerate(data):
        if i == 8:
            break
        result = (result << shift) | (byte & 0x7F)
        shift += 7
        if not byte & 0x80:
            return result, data[i + 1 :]
    # Not tested!
    result = (result << 8) | byte
    return result, data[8:]


def varints(data):
    """
    Consume all data by reading varints. Will give invalid output
    for invalid data.
    """
    results = []
    while data:
        i, data = varint(data)
        results.append(i)
    return results


def serial_type_from_int(n):
    "Return a tuple of (serial type, n_bytes) as described in '2.1. Record Format'"
    if n == 0:
        return ("null", 0)
    if 1 <= n <= 6:
        # Value is a big-endian (n * 8)-bit twos-complement integer.
        return ("int", n)
    if n == 7:
        # IEEE 754-2008 64-bit floating point number.
        return ("float", 8)
    # if n == 8:
    #     return 0
    # if n == 0:
    #     return 1
    if n >= 12 and n % 2 == 0:
        return ("blob", (n - 12) // 2)
    if n >= 13 and n % 2 == 1:
        return ("string", (n - 13) // 2)


def read_serial(serial_type, data):
    """
    Decode data given the serial type and some bytes.
    """
    match serial_type:
        case ("string", n):
            return data[:n].decode("utf-8")
        case ("int", n):
            return int.from_bytes(data[:n], byteorder="big", signed=False)


def read_table_btree_leaf_header(payload):
    size, _ = varint(payload)
    header = payload[:size]
    # Drop the header size from the header, since we already have it
    # and are collecting serial types now.
    _, header = varint(header)
    serial_types = []
    while header:
        n, header = varint(header)
        serial_types.append(serial_type_from_int(n))
    return size, serial_types


def read_table_btree_leaf_body(data, serial_types):
    values = []
    for st in serial_types:
        values.append(read_serial(st, data))
        _, bytes_consumed = st
        data = data[bytes_consumed:]
    return values


def read_table_btree_leaf_cell(page, offset):
    payload_size, rest = varint(page[offset:])
    row_id, rest = varint(rest)
    # Here is where you check if the payload spills onto overflow
    # pages. This is a calculation involving the pagesize, the offset
    # of this cell, the number of bytes used by the first two varints
    # and the size of the payload.
    # For now assume that there is no overflow.
    payload = rest[:payload_size]
    header_size, serial_types = read_table_btree_leaf_header(payload)
    return read_table_btree_leaf_body(payload[header_size:], serial_types)


if __name__ == "__main__":
    database_header = read_database_header(database_file_path)
    pagesize = database_header["page_size"]
    # Assert we are UTF-8
    assert database_header["text_encoding"] == 1
    sqlite_schema_page = read_page(database_file_path, pagesize, 0)
    sqlite_schema_page_header = read_page_header(sqlite_schema_page, True)
    if command == ".dbinfo":
        print(f"Magic string: {database_header['magic_header_string']}")
        print(f"database page size: {pagesize}")
        print(f"number of tables: {sqlite_schema_page_header['n_cells']}")
    elif command == ".tables":
        offsets = cell_pointer_offsets(sqlite_schema_page, sqlite_schema_page_header['n_cells'], True)
        table_names = []
        for offset in offsets:
            body = read_table_btree_leaf_cell(sqlite_schema_page, offset)
            table_names.append(body[2])
        if "sqlite_sequence" in table_names:
            table_names.remove("sqlite_sequence")
        print(" ".join(table_names))

        # print(record_body[:40])
    else:
        print(f"Invalid command: {command}")
