# I write and test methods one at a time.
# This file contains unit tests of individual methods.

from src.CSVDataTable import CSVDataTable
from src.RDBDataTable import RDBDataTable
import logging
import os

# The logging level to use should be an environment variable, not hard coded.
logging.basicConfig(level=logging.DEBUG)

# Also, the 'name' of the logger to use should be an environment variable.
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# This should also be an environment variable.
# Also not the using '/' is OS dependent, and windows might need `\\`
data_dir = os.path.abspath("../Data/Baseball")


def test_load():

    print("\n")
    print("******************** " + "test_load" + " ********************")
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }
    csv_tbl = CSVDataTable("people", connect_info, None, debug=False)
    print("Created table = " + str(csv_tbl))
    key_fields = csv_tbl.get_key_fields_list()
    print("Primary Keys:", key_fields)
    all_fields = csv_tbl.get_fieldnames_list()
    print("All Fields: ", all_fields)
    print("******************** " + "end test_load" + " ********************")
    return csv_tbl


def test_connect():

    print("\n")
    print("******************** " + "test_load" + " ********************")
    connect_info = dict(host="localhost", user="someone", password="link", database="lahman")
    rdb_tbl = RDBDataTable("People", connect_info, ["playerID"])
    key_fields = rdb_tbl.get_key_fields_list()
    print("Primary Keys:", key_fields)
    all_fields = rdb_tbl.get_fieldnames_list()
    print("All Fields: ", all_fields)
    print("******************** " + "end test_load" + " ********************")
    return rdb_tbl


def t_query_template():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, None, debug=False)

    template = {'playerID': 'aardsda01', 'birthYear': '1981', 'birthMonth': '12'}
    fields = ['retroID', 'bbrefID']
    result = csv_tbl.find_by_template(template, fields)
    print("testing 'find_by_template':", result)
    correct_result = [dict([('retroID', 'aardd001'), ('bbrefID', 'aardsda01')])]
    assert result == correct_result


def t_query_primary_key():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, None)

    result = csv_tbl.find_by_primary_key(["aardd001"])

    print(result)

    # TODO correct ones.


t1 = test_load()
t2 = test_connect()
