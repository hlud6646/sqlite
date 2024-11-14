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
from itertools import compress

from .select import parse_select, make_predicate

DATABASE_FILE_PATH = sys.argv[1]
COMMAND = sys.argv[2]


# Refer to the SQLite file format at https://www.sqlite.org/fileformat.html
DATABASE_HEADER_BYTES = 100
PAGE_HEADER_BYTES = 8
LEAF_PAGE_HEADER_BYTES = 8
PAGE_SIZE_OFFSET = 16
CELL_COUNT_OFFSET = 3


def read_database_header():
    with open(DATABASE_FILE_PATH, "rb") as db_file:
        data = db_file.read(DATABASE_HEADER_BYTES)
        # Refer to '1.3. The Database Header' for the offsets/lengths.
        return {
            "magic_header_string": data[:16],
            "page_size": int.from_bytes(data[16:18]),
            "text_encoding": int.from_bytes(data[56:60]),
            # Extend this parsing as needed.
        }


def read_page(page_number):
    """Read the page. Note that they start counting at 1."""
    with open(DATABASE_FILE_PATH, "rb") as db_file:
        db_file.seek(PAGESIZE * (page_number - 1))
        return db_file.read(PAGESIZE)


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


def schema_table_rows():
    page_header = read_page_header(SCHEMA_PAGE, True)
    offsets = cell_pointer_offsets(SCHEMA_PAGE, page_header['n_cells'], True)
    return (read_table_btree_leaf_cell(SCHEMA_PAGE, o) for o in offsets)


def count_rows(page):
    header = read_page_header(page)
    return len(cell_pointer_offsets(page, header['n_cells']))


p_select = re.compile(
    r"""
    ^select\s
    (?P<col_names>((\w+,\s)+\w+)|\w+)\s
    from\s
    (?P<tbl_name>\w+)
    (\s
    where\s
    (?P<condition>(.+))
    )?
    $
    """,
    re.VERBOSE | re.IGNORECASE
)


def handle_dbinfo():
    print(f"Magic string: {DATABASE_HEADER['magic_header_string']}")
    print(f"database page size: {DATABASE_HEADER['page_size']}")
    print(f"number of tables: {SCHEMA_PAGE_HEADER['n_cells']}")


def handle_tables():
    table_names = [row[2] for row in schema_table_rows()]
    if "sqlite_sequence" in table_names:
        table_names.remove("sqlite_sequence")
    print(" ".join(table_names))


def handle_count(tbl_name):
    rootpage_number = next(r for r in schema_table_rows() if r[2] == tbl_name)[3]
    rootpage = read_page(rootpage_number)
    print(count_rows(rootpage))



def get_rootpage_number(table_name):
    schema_table_row = next(r for r in schema_table_rows() if r[2] == table_name)
    return schema_table_row[3]


def get_column_names(table_name):
    schema_table_row = next(r for r in schema_table_rows() if r[2] == table_name)
    table_sql = schema_table_row[-1]
    table_sql = list(sqlparse.parse(table_sql)[0])
    cols = next(token for token in table_sql if token.value.startswith("("))
    cols = cols.value[1:-1].split(',')
    cols = [c.strip().split(" ") for c in cols]
    col_names = [c[0] for c in cols]
    return col_names[1:]



def read_table(table_name):

    # Reading the specified table
    rootpage_number = get_rootpage_number(table_name)
    rootpage = read_page(rootpage_number)
    header = read_page_header(rootpage)
    cpos = cell_pointer_offsets(rootpage, header['n_cells'])
    table = [read_table_btree_leaf_cell(rootpage, o)[1:] for o in cpos]
    column_names = get_column_names(table_name)
    return dict(zip(column_names, zip(*table)))


def handle_select(command):
    select_exprs, table_name, condition = parse_select(command)
    table = read_table(table_name)

    # Apply a WHERE... clause to the table
    column_names = table.keys()
    if condition:
        col, predicate = make_predicate(condition)
        includes = list(map(predicate, table[col]))
        tmp = zip(*[table[k] for k in table])
        tmp = compress(tmp, includes)
        table = dict(zip(column_names, zip(*tmp)))

    # Collect the columns that appear in the select_statement
    table = {k:table[k] for k in (e[1] for e in select_exprs)}
    table = zip(*table.values())    
    for row in table:
        print("|".join(row))

        
DATABASE_HEADER = read_database_header()
PAGESIZE = DATABASE_HEADER["page_size"]
assert DATABASE_HEADER["text_encoding"] == 1  # Assert we are UTF-8
SCHEMA_PAGE = read_page(1)
SCHEMA_PAGE_HEADER = read_page_header(SCHEMA_PAGE, True)

match COMMAND:
    case ".dbinfo":
        handle_dbinfo()
    case ".tables":
        handle_tables()
    # Specialised select for this type of query.
    # Delete in favour of fewer specialisations or keep?
    case str() if COMMAND.lower().startswith("select count(*) from"):
        tbl_name = COMMAND.split(" ")[-1]
        handle_count(tbl_name)
    case str() if COMMAND.lower().startswith("select"):
        handle_select(COMMAND)

    case _:
        print(f"Invalid command: {COMMAND}")
