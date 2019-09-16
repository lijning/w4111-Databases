# I write and test methods one at a time.
# This file contains unit tests of individual methods.

from src.CSVDataTable import CSVDataTable
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


def t_load():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, None)

    print("Created table = " + str(csv_tbl))


def t_fields_meta():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, None)

    key_fields = csv_tbl.get_key_fields_list()
    print("Primary Keys:", key_fields)
    all_fields = csv_tbl.get_fieldnames_list()
    print("All Fields: ", all_fields)


def t_query_template():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, None)

    template = {'playerID': 'aardsda01', 'birthYear': '1981', 'birthMonth': '12'}
    fields = ['retroID', 'bbrefID']
    result = csv_tbl.find_by_template(template, fields)
    print("testing 'find_by_template':", result)
    correct_result = [dict([('retroID', 'aardd001'), ('bbrefID', 'aardsda01')])]
    assert result == correct_result


t_load()
t_fields_meta()
t_query_template()
