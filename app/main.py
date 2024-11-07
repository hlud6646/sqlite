"""
Building SQLite.

What have I learned in this project:
 - SQLite file format;
 - Navigating binary files;
 - Bitwise operations;
"""

import sys
import re
import sqlparse
from operator import itemgetter


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


def read_page(database_file_path, pagesize, page_number):
    """Read the page. Note that they start counting at 1."""
    with open(database_file_path, "rb") as db_file:
        db_file.seek(pagesize * (page_number - 1))
        return db_file.read(pagesize)


def read_page_header(page, skip_db_header=False, interior=False):
    # The first page in a database file contains the database header.
    # If you want to read the page header of this page, you need to read
    # past the database header first.
    if skip_db_header:
        page = page[DATABASE_HEADER_BYTES:]
    if interior:
        header = page[:12]
    else:
        header = page[:8]
    return {
        "page_type": header[0],
        "n_cells": int.from_bytes(header[3:3 + 2]),
        "cell_content_area": int.from_bytes(header[5:5 + 2])
    }


def cell_pointer_offsets(page, n_cells, skip_db_header=False):
    if skip_db_header:
        page = page[DATABASE_HEADER_BYTES:]
    page = page[PAGE_HEADER_BYTES:]
    offsets = []
    for i in range(n_cells):
        offsets.append(int.from_bytes(page[i * 2:(i + 1) * 2]))
    return offsets


def varint(data):
    """Decode a single varint from bytes and return it and the rest of the data."""
    result = 0
    shift = 0

    for i, byte in enumerate(data):
        if i == 8:
            break
        result = (result << shift) | (byte & 0x7F)
        shift += 7
        if not byte & 0x80:
            return result, data[i + 1:]
    # Not tested!
    result = (result << 8) | byte
    return result, data[8:]


def varints(data):
    """Consume all data by reading varints. Will give invalid output for invalid data."""
    results = []
    while data:
        i, data = varint(data)
        results.append(i)
    return results


def serial_type_from_int(n):
    """Return a tuple of (serial type, n_bytes) as described in '2.1. Record Format'"""
    if n == 0:
        return ("NULL", 0)
    if 1 <= n <= 6:
        # Value is a big-endian (n * 8)-bit twos-complement integer.
        return ("int", n)
    if n == 7:
        # IEEE 754-2008 64-bit floating point number.
        return ("float", 8)
    if n >= 12 and n % 2 == 0:
        return ("blob", (n - 12) // 2)
    if n >= 13 and n % 2 == 1:
        return ("string", (n - 13) // 2)


def read_serial(serial_type, data):
    """Decode data given the serial type and some bytes."""
    match serial_type:
        case ("NULL", 0):
            return "NULL"
        case ("string", n):
            return data[:n].decode("utf-8")
        case ("int", n):
            return int.from_bytes(data[:n], byteorder="big", signed=False)


def read_table_btree_leaf_cell_header(payload):
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


def read_table_btree_leaf_cell_body(data, serial_types):
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
    header_size, serial_types = read_table_btree_leaf_cell_header(payload)
    return read_table_btree_leaf_cell_body(payload[header_size:], serial_types)


def read_schema_table_row():
    pass


def schema_table_rows(schema_page):
    page_header = read_page_header(schema_page, True)
    offsets = cell_pointer_offsets(schema_page, page_header['n_cells'], True)
    return (read_table_btree_leaf_cell(schema_page, o) for o in offsets)


def count_rows(page):
    header = read_page_header(page)
    return len(cell_pointer_offsets(page, header['n_cells']))


p_select = re.compile(
    r"""
    ^select\s
    (?P<col_names>((\w+,\s)+\w+)|\w+)\s
    from\s
    (?P<tbl_name>\w+)$
    """,
    re.VERBOSE | re.IGNORECASE
)


def handle_dbinfo(database_header, schema_page_header):
    print(f"Magic string: {database_header['magic_header_string']}")
    print(f"database page size: {database_header['page_size']}")
    print(f"number of tables: {schema_page_header['n_cells']}")


def handle_tables(schema_page):
    table_names = [row[2] for row in schema_table_rows(schema_page)]
    if "sqlite_sequence" in table_names:
        table_names.remove("sqlite_sequence")
    print(" ".join(table_names))


def handle_count(database_file_path, pagesize, schema_page, tbl_name):
    rootpage_number = next(r for r in schema_table_rows(schema_page) if r[2] == tbl_name)[3]
    rootpage = read_page(database_file_path, pagesize, rootpage_number)
    print(count_rows(rootpage))


def handle_select(database_file_path, pagesize, schema_page, column_names, table_name):
    rootpage = next(r for r in schema_table_rows(schema_page) if r[2] == table_name)
    rootpage_number = rootpage[3]
    rootpage_sql = rootpage[-1]
    sql = list(sqlparse.parse(rootpage_sql)[0])

    # We need to get the column names from the sql statement that created the
    # table, in order to retrieve the index of the requested column.
    cols = next(token for token in sql if token.value.startswith("("))
    cols = cols.value[1:-1].split(',')
    cols = [c.strip().split(" ") for c in cols]
    col_names = [c[0] for c in cols]
    col_indicies = list(map(lambda name: col_names.index(name), column_names))

    rootpage = read_page(database_file_path, pagesize, rootpage_number)
    header = read_page_header(rootpage)
    cpos = cell_pointer_offsets(rootpage, header['n_cells'])
    for o in cpos:
        if len(col_indicies) == 1:
            print(read_table_btree_leaf_cell(rootpage, o)[col_indicies[0]])
        else:
            values = itemgetter(*col_indicies)(read_table_btree_leaf_cell(rootpage, o))
            print("|".join(values))


if __name__ == "__main__":
    database_header = read_database_header(database_file_path)
    pagesize = database_header["page_size"]
    assert database_header["text_encoding"] == 1  # Assert we are UTF-8
    schema_page = read_page(database_file_path, pagesize, 1)
    schema_page_header = read_page_header(schema_page, True)

    match command:
        case ".dbinfo":
            handle_dbinfo(database_header, schema_page_header)
        case ".tables":
            handle_tables(schema_page)
        case str() if command.lower().startswith("select count(*) from"):
            tbl_name = command.split(" ")[-1]
            handle_count(database_file_path, pagesize, schema_page, tbl_name)
        case str() if (match := re.match(p_select, command)):
            handle_select(
                database_file_path,
                pagesize,
                schema_page,
                match.group('col_names').split(', '),
                match.group('tbl_name')
            )
        case _:
            print(f"Invalid command: {command}")
