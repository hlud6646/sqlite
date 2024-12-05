import sys
import sqlparse
from itertools import compress

from .select import parse_select, make_predicate
from .utils import combine_dicts
from .serial_types import read_serial, serial_type_from_int
from .varint import read_varint

# -- Constants.
#    Refer to the SQLite file format at https://www.sqlite.org/fileformat.html


DATABASE_HEADER_BYTES = 100
INTERIOR_PAGE_HEADER_BYTES = 12
LEAF_PAGE_HEADER_BYTES = 8
PAGE_SIZE_OFFSET = 16
CELL_COUNT_OFFSET = 3


# END


# -- Database and page headers.


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


def read_page_header(page, skip_db_header=False):
    if skip_db_header:
        page = page[DATABASE_HEADER_BYTES:]
    page_type = page[:1]
    interior = page_type in b"\x02\x05"
    header = {
        "page_type": page[0],
        "n_cells": int.from_bytes(page[3 : 3 + 2]),
        "cell_content_area": int.from_bytes(page[5 : 5 + 2]),
    }
    if interior:
        header["right_most_pointer"] = page[8 : 8 + 4]
    return header


# END


# -- Low level access of pages/cells.


def page_bytes(page_number):
    """Read the page. Note that they start counting at 1.
    This function simply returns the bytes holding the page data."""
    with open(DATABASE_FILE_PATH, "rb") as db_file:
        db_file.seek(PAGESIZE * (page_number - 1))
        return db_file.read(PAGESIZE)


def read_cell_pointer_offsets(page, page_header, skip_db_header=False):
    """Return a list of cell pointer offsets.
    Note that these are absolute (i.e. not relative to the page being read.)"""
    if skip_db_header:
        page = page[DATABASE_HEADER_BYTES:]
    if page_header["page_type"] in b"\x0d\x0a":
        page = page[LEAF_PAGE_HEADER_BYTES:]
    else:
        page = page[INTERIOR_PAGE_HEADER_BYTES:]
    offsets = []
    for i in range(page_header["n_cells"]):
        offsets.append(int.from_bytes(page[i * 2 : (i + 1) * 2]))
    return offsets


# END


# -- Read a single table B-tree leaf cell.


def read_table_btree_leaf_cell_header(payload):
    """Helper function for read_table_btree_leaf_cell."""
    size, _ = read_varint(payload)
    header = payload[:size]
    # Drop the header size from the header, since we already have it
    # and are collecting serial types now.
    _, header = read_varint(header)
    serial_types = []
    while header:
        n, header = read_varint(header)
        serial_types.append(serial_type_from_int(n))
    return size, serial_types


def read_table_btree_leaf_cell_body(rowid, data, serial_types):
    """Helper function for read_table_btree_leaf_cell."""
    values = []
    for st in serial_types:
        values.append(read_serial(st, data))
        _, bytes_consumed = st
        data = data[bytes_consumed:]
    values[0] = rowid
    return values


def read_table_btree_leaf_cell(page, offset):
    """Return the data held by the cell at the given offset.
    That is, return a list containing the valus in the corresponding row of the database.
    """
    payload_size, rest = read_varint(page[offset:])
    rowid, rest = read_varint(rest)
    # Here is where you check if the payload spills onto overflow
    # pages. This is a calculation involving the pagesize, the offset
    # of this cell, the number of bytes used by the first two varints
    # and the size of the payload.
    # For now assume that there is no overflow.
    payload = rest[:payload_size]
    header_size, serial_types = read_table_btree_leaf_cell_header(payload)
    return read_table_btree_leaf_cell_body(rowid, payload[header_size:], serial_types)


# END


# -- Table reading utils.


def schema_table_rows():
    page_header = read_page_header(SCHEMA_PAGE, True)
    offsets = read_cell_pointer_offsets(SCHEMA_PAGE, page_header, True)
    return (read_table_btree_leaf_cell(SCHEMA_PAGE, o) for o in offsets)


def count_rows(page):
    header = read_page_header(page)
    return len(read_cell_pointer_offsets(page, header))


def get_rootpage_number(table_name):
    schema_table_row = next(r for r in schema_table_rows() if r[2] == table_name)
    return schema_table_row[3]


def get_column_names(table_name):
    schema_table_row = next(r for r in schema_table_rows() if r[2] == table_name)
    table_sql = schema_table_row[-1]
    table_sql = list(sqlparse.parse(table_sql)[0])
    cols = next(token for token in table_sql if token.value.startswith("("))
    cols = cols.value[1:-1].split(",")
    cols = [c.strip().split(" ") for c in cols]
    col_names = [c[0] for c in cols]
    return col_names


# END


# -- Read a B-tree table.
#    This section really just contains a standard b-tree traversal, with the
#    traversal logic for interior/leaf nodes defined outside the main function.


def read_btree_leaf(page_number, table_name):
    """Read the cells on a table B-tree leaf page."""
    page = page_bytes(page_number)
    header = read_page_header(page)
    cpos = read_cell_pointer_offsets(page, header)
    table = [read_table_btree_leaf_cell(page, o) for o in cpos]
    column_names = get_column_names(table_name)
    return dict(zip(column_names, zip(*table)))


def read_table_btree_interior_cell(page, offset):
    left_child = int.from_bytes(page[offset : offset + 4])
    key, _ = read_varint(page[offset + 4 :])
    return left_child, key


def traverse(page_number, table_name):
    """Recrursive traversal of a B-Tree"""
    page = page_bytes(page_number)
    header = read_page_header(page)

    if header["page_type"] == 13:  # leaf
        return read_btree_leaf(page_number, table_name)
    elif header["page_type"] == 5:  # interior
        chunks = []
        for cpo in read_cell_pointer_offsets(page, header):
            left_child, key = read_table_btree_interior_cell(page, cpo)
            chunks.append(traverse(left_child, table_name))
        return combine_dicts(chunks)
    else:
        raise ValueError(
            f"Page type must be 13 (leaf) or 5 (interior), got {header['page_type']}"
        )


# END


# -- Handlers for CLI


def handle_dbinfo():
    print(f"Magic string: {DATABASE_HEADER['magic_header_string']}")
    print(f"database page size: {DATABASE_HEADER['page_size']}")
    print(f"number of tables: {SCHEMA_PAGE_HEADER['n_cells']}")


def handle_tables():
    table_names = [row[2] for row in schema_table_rows()]
    if "sqlite_sequence" in table_names:
        table_names.remove("sqlite_sequence")
    print(" ".join(table_names))


# TODO: This is simple, but only works for single page tables.
def handle_count(tbl_name):
    rootpage_number = next(r for r in schema_table_rows() if r[2] == tbl_name)[3]
    rootpage = page_bytes(rootpage_number)
    print(count_rows(rootpage))


# TODO: Handle select *
# TODO: Handle aggregate functions in select expressions. This would
#       absorb the seperate handling of SELECT COUNT statements.
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
    table = {k: table[k] for k in (e[1] for e in select_exprs)}
    table = zip(*table.values())
    for row in table:
        print("|".join((str(elem) for elem in row)))


# END


if __name__ == "__main__":

    DATABASE_FILE_PATH = sys.argv[1]
    COMMAND = sys.argv[2]
    DATABASE_HEADER = read_database_header()
    PAGESIZE = DATABASE_HEADER["page_size"]
    assert DATABASE_HEADER["text_encoding"] == 1  # Assert we are UTF-8
    SCHEMA_PAGE = page_bytes(1)
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
