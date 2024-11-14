"""
Building SQLite.

What have I learned in this project:
 - SQLite file format;
 - Navigating binary files;
 - Bitwise operations;


General TODOS:
 - Organise project better into constants, utils (eg read varint and combine_dicts),
 read functions (those that open
 the file), interior functions, leaf functions, handlers, main
 - formatting
 - documenting; some of the stuff that goes on here is downright weird. this is not 
 bad code, rather sqlite itself is pretty weird. this justifies extra documentation 
 in the code (it will be hard to make self evident code sometimes)
 - raise informative errors when appropriate, like in serial_type_from_int.
"""

import sys
import re
import sqlparse
from itertools import compress
from collections import defaultdict
from .select import parse_select, make_predicate
from pprint import pprint

# Refer to the SQLite file format at https://www.sqlite.org/fileformat.html
DATABASE_HEADER_BYTES = 100
INTERIOR_PAGE_HEADER_BYTES = 12
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


def read_page_header(page, skip_db_header=False):
    if skip_db_header:
        page = page[DATABASE_HEADER_BYTES:]
    page_type = page[:1]
    interior = page_type in b'\x02\x05'
    header = {
        "page_type": page[0],
        "n_cells": int.from_bytes(page[3:3 + 2]),
        "cell_content_area": int.from_bytes(page[5:5 + 2])
    }
    if interior:
        header['right_most_pointer'] = page[8:8 + 4]
    return header


def cell_pointer_offsets(page, page_header, skip_db_header=False):
    if skip_db_header:
        page = page[DATABASE_HEADER_BYTES:]
    if page_header['page_type'] in b'\x0d\x0a':
        page = page[LEAF_PAGE_HEADER_BYTES:]
    else:
        page = page[INTERIOR_PAGE_HEADER_BYTES:]
    offsets = []
    for i in range(page_header['n_cells']):
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


# TODO: Convert to a match
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
    if n == 8:
        return ("int", 0)
    if n == 9:
        return ("int", 1)
    if n >= 12 and n % 2 == 0:
        return ("blob", (n - 12) // 2)
    if n >= 13 and n % 2 == 1:
        return ("string", (n - 13) // 2)
    raise ValueError(f"Unknown serial type specifier: {n}")



def read_serial(serial_type, data):
    """Decode data given the serial type and some bytes."""
    match serial_type:
        case ("NULL", 0):
            return "NULL"
        case ("string", n):
            return data[:n].decode("utf-8")
        case ("int", n):
            return int.from_bytes(data[:n], byteorder="big", signed=False)
        case _:
            raise ValueError(f"Unknown serial type: {serial_type}")


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


def read_table_btree_leaf_cell_body(rowid, data, serial_types):
    values = []
    for st in serial_types:
        values.append(read_serial(st, data))
        _, bytes_consumed = st
        data = data[bytes_consumed:]
    values[0] = rowid
    return values


def read_table_btree_leaf_cell(page, offset):
    payload_size, rest = varint(page[offset:])
    rowid, rest = varint(rest)
    # Here is where you check if the payload spills onto overflow
    # pages. This is a calculation involving the pagesize, the offset
    # of this cell, the number of bytes used by the first two varints
    # and the size of the payload.
    # For now assume that there is no overflow.
    payload = rest[:payload_size]
    header_size, serial_types = read_table_btree_leaf_cell_header(payload)
    return read_table_btree_leaf_cell_body(rowid, payload[header_size:], serial_types)


def read_table_btree_interior_cell(page, offset):
    left_child = int.from_bytes(page[offset: offset + 4])
    key, _ = varint(page[offset + 4:])
    return left_child, key


def schema_table_rows():
    page_header = read_page_header(SCHEMA_PAGE, True)
    offsets = cell_pointer_offsets(SCHEMA_PAGE, page_header, True)
    return (read_table_btree_leaf_cell(SCHEMA_PAGE, o) for o in offsets)


def count_rows(page):
    header = read_page_header(page)
    return len(cell_pointer_offsets(page, header))



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
    return col_names


def read_table(table_name):
    """Read the cells on a table B-tree leaf page."""
    rootpage_number = get_rootpage_number(table_name)
    rootpage = read_page(rootpage_number)
    header = read_page_header(rootpage)
    cpos = cell_pointer_offsets(rootpage, header)
    table = [read_table_btree_leaf_cell(rootpage, o) for o in cpos]
    column_names = get_column_names(table_name)
    return dict(zip(column_names, zip(*table)))




def read_btree_leaf(page_number, table_name):
    """Read the cells on a table B-tree leaf page."""
    page = read_page(page_number)
    header = read_page_header(page)
    cpos = cell_pointer_offsets(page, header)
    table = [read_table_btree_leaf_cell(page, o) for o in cpos]
    column_names = get_column_names(table_name)
    return dict(zip(column_names, zip(*table)))



# TODO: Move to utils section.
def combine_dicts(dict_list):
    """Given an iterable of dicts, with all lists as values, 
    combine into one dict by joning the lists."""
    result = defaultdict(list)
    for d in dict_list:
        for key, values in d.items():
            result[key].extend(values)
    return dict(result)


def traverse(page_number, table_name):
    """Recrursive traversal of a B-Tree"""
    page = read_page(page_number)
    header = read_page_header(page)

    # Base Case is that the page is a leaf.
    if header['page_type'] == 13:
        # Rename and refactor function read_table. At the moment it is 
        # doing all the work for reading a leaf table, since those are 
        # all we have encountered so far. When used in this function, 
        # which will cover both leaf and interior cases, it can pick up from 
        # rootpage and header, i.e. can refactor out read_page and read_page_header.
        return read_btree_leaf(page_number, table_name)
    elif header['page_type'] == 5:
        chunks = []
        for cpo in cell_pointer_offsets(page, header):
            left_child, key = read_table_btree_interior_cell(page, cpo)
            chunks.append(traverse(left_child, table_name))
        return combine_dicts(chunks)            


def handle_select():
    """Traverse a B-Tree and collect the rows, no index."""
    select_exprs, table_name, condition = parse_select(COMMAND)
    rootpage_number = get_rootpage_number(table_name)
    table = traverse(rootpage_number, table_name)

    # Apply a WHERE... clause to the table
    column_names = table.keys()
    if condition:
        col, predicate = make_predicate(condition)
        includes = list(map(predicate, table[col]))
        tmp = zip(*[table[k] for k in table])
        tmp = compress(tmp, includes)
        table = dict(zip(column_names, zip(*tmp)))


    # Collect the columns that appear in the select_statement.
    # This desing is future-proofed against applying functions
    # in the select expressions.
    table = {k:table[k] for k in (e[1] for e in select_exprs)}
    table = zip(*table.values())    
    for row in table:
        print("|".join((str(elem) for elem in row)))




if __name__ == "__main__":
            
    DATABASE_FILE_PATH = sys.argv[1]
    COMMAND = sys.argv[2]
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
            handle_select()
        case _:
            print(f"Invalid command: {COMMAND}")










# ARCHIVE



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
