# I write and test methods one at a time.
# This file contains unit tests of individual methods.

from src.CSVDataTable import CSVDataTable
from src.RDBDataTable import RDBDataTable, SqlHelper
from src.BaseDataTable import DataTableError, BaseDataTable
import logging
import os
import json

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
    key_fields = csv_tbl.get_key_fields_list()
    print("Primary Keys:", key_fields)
    all_fields = csv_tbl.get_fieldnames_list()
    print("All Fields: ", all_fields)
    print("******************** " + "end test_load" + " ********************")
    return csv_tbl


def test_connect():
    print("\n")
    print("******************** " + "test connect" + " ********************")
    connect_info = dict(host="localhost", user="someone", password="link", database="lahman")
    rdb_tbl = RDBDataTable("People", connect_info, ["playerID"])
    key_fields = rdb_tbl.get_key_fields_list()
    print("Primary Keys:", key_fields)
    all_fields = rdb_tbl.get_fieldnames_list()
    print("All Fields: ", all_fields)
    print("******************** " + "end test connect" + " ********************")
    return rdb_tbl


def t2_connect():
    print("\n")
    print("******************** " + "test connect 2" + " ********************")
    connect_info = dict(host="localhost", user="someone", password="link", database="lahman")

    rdb_tbl = RDBDataTable("Appearances", connect_info, ["playerID"])
    if len(rdb_tbl.get_key_fields_list()) == 4:
        print("Correct key columns, when given wrong ones.")

    rdb_tbl = RDBDataTable("Appearances", connect_info, ['yearID', 'teamID', 'lgID', 'playerID'])
    key_fields = rdb_tbl.get_key_fields_list()
    print("Primary Keys:", key_fields)
    all_fields = rdb_tbl.get_fieldnames_list()
    print("All Fields: ", all_fields)
    print("******************** " + "end test connect" + " ********************")
    return rdb_tbl


def test_helper(csv: CSVDataTable, rdb: RDBDataTable):
    print("\n")
    print("******************** " + "test helper" + " ********************")
    print("# matches_template:")
    assert not csv.matches_template({'playerID': "aardsda99", "birthYear": "1933", "deathYear": "2099"},
                                    {"deathYear": "2099", "birthYear": "1938"})

    cur = rdb.get_cursor()
    print("# construct_insert:")
    insert, args = SqlHelper.construct_insert({"name": "Jordan", "year": 1988}, "People")
    print(cur.mogrify(insert, args))
    print("# construct_update:")
    update, args = SqlHelper.construct_update({"name": "Jordan", "year": 1988},
                                              {"birthYear": "1988", "deathYear": 2004}, "People")
    print(cur.mogrify(update, args))
    print("******************** " + "end test helper" + " ********************")


def test_query_template(csv: CSVDataTable, rdb: RDBDataTable):
    print("\n")
    print("******************** " + "test query template" + " ********************")
    template = {'playerID': 'aardsda01', 'birthYear': '1981', 'birthMonth': '12'}
    fields = ['retroID', 'bbrefID']
    correct_result = [dict([('retroID', 'aardd001'), ('bbrefID', 'aardsda01')])]
    csv_result = csv.find_by_template(template, fields)
    rdb_result = rdb.find_by_template(template, fields)
    print("# csv_result: " + json.dumps(csv_result[:1]))
    print("# rdb_result: " + json.dumps(rdb_result[:1]))

    assert csv_result == rdb_result == correct_result
    print("******************** " + "end test query template" + " ********************")


def test_query_pk(csv: CSVDataTable, rdb: RDBDataTable):
    print("\n")
    print("******************** " + "test query pk" + " ********************")
    csv_result = csv.find_by_primary_key(["aardsda01"])
    rdb_result = rdb.find_by_primary_key(["aardsda01"])

    try:
        assert csv_result == rdb_result
    except AssertionError:
        logger.error("test_query_pk: unexpected result.")
        print("# csv_result: " + json.dumps(csv_result))
        print("# rdb_result: " + json.dumps(rdb_result))
    finally:
        print("******************** " + "end test query pk" + " ********************")


def test_insert_duplicate(csv: CSVDataTable, rdb: RDBDataTable):
    print("\n")
    print("******************** " + "test insert duplicate" + " ********************")
    try:
        csv.insert({'playerID': "aardsda01", "birthYear": "1933"})
    except DataTableError:
        print("CSV: Duplicate insertion detected.")
    else:
        raise AssertionError

    try:
        rdb.insert({'playerID': "aardsda01", "birthYear": "1933"})
    except DataTableError:
        print("RDB: Duplicate insertion detected.")
    else:
        raise AssertionError

    print("******************** " + "end test insert" + " ********************")


def test_insert_delete_template(tbl: BaseDataTable):
    print("\n")
    print("******************** " + "test insert template" + " ********************")
    result = tbl.find_by_template({"birthYear": "1933"})
    print("Num. of rows before insertion: %s" % len(result))
    tbl.insert({'playerID': "aardsda99", "birthYear": "1933", "deathYear": "2099"})
    result = tbl.find_by_template({"birthYear": "1933"})
    print("Num. of rows after insertion: %s" % len(result))
    tbl.delete_by_template({"birthYear": "1933", "deathYear": "2099"})
    result = tbl.find_by_template({"birthYear": "1933"})
    print("Num. of rows after deletion: %s" % len(result))
    print("******************** " + "end test insert" + " ********************")


def test_insert_delete_pk(tbl: BaseDataTable):
    print("\n")
    print("******************** " + "test insert pk" + " ********************")
    sample_pk = ["aardsda99"]
    result = tbl.find_by_primary_key(sample_pk)
    assert result is None
    tbl.insert({'playerID': "aardsda99", "birthYear": "1933", "deathYear": "2099"})
    result = tbl.find_by_primary_key(sample_pk)
    assert result is not None
    assert tbl.delete_by_key(sample_pk) == 1
    result = tbl.find_by_primary_key(sample_pk)
    assert result is None
    print("******************** " + "end test insert" + " ********************")


def test_update_tpl(tbl: BaseDataTable):
    print("\n")
    print("******************** " + "test update tpl" + " ********************")
    tbl.insert({'playerID': "aardsda99", "birthYear": "1933", "deathYear": "2099"})
    print("before update:")
    print(tbl.find_by_template({"deathYear": "2099"}))
    response = tbl.update_by_template({'playerID': "aardsda99", "birthYear": "1933"},
                                      {"birthYear": "1655"})
    print("%s rows affected." % response)
    print("after  update:")
    print(tbl.find_by_template({"deathYear": "2099"}))
    assert 1 == tbl.delete_by_key(["aardsda99"])
    print("******************** " + "end test update" + " ********************")


def test_update_pk(tbl: BaseDataTable):
    print("\n")
    print("******************** " + "test update pk" + " ********************")
    tbl.insert({'playerID': "aardsda99", "birthYear": "1933", "deathYear": "2099"})
    print("before update:")
    print(tbl.find_by_template({"deathYear": "2099"}))
    response = tbl.update_by_key(["aardsda99"], {"birthYear": "1655"})
    print("%s rows affected." % response)
    print("after  update:")
    print(tbl.find_by_template({"deathYear": "2099"}))
    assert 1 == tbl.delete_by_key(["aardsda99"])
    print("******************** " + "end test update" + " ********************")


def t2_multi_pk(tbl: BaseDataTable):
    print("\n")
    print("******************** " + "test multi pk" + " ********************")
    new_values = [{"yearID": "1560", "teamID": "xxx", "lgID": "QQ", "playerID": "weitao99"},
                  {"yearID": "1561", "teamID": "xxx", "lgID": "QQ", "playerID": "weitao99"},
                  {"yearID": "1561", "teamID": "xxx", "lgID": "QQ", "playerID": "weitao98"},
                  {"yearID": "1560", "teamID": "xxx", "lgID": "QQ", "playerID": "weitao99"}]
    for val in new_values:
        try:
            tbl.insert(val)
        except Exception as err:
            print("Invalid insert, causing duplicate pk: ", err)
        else:
            print("inserted:", val.values())

    result = tbl.find_by_template({"teamID": "xxx"})
    print("Num. of records in team 'xxx': ", len(result))
    try:
        tbl.update_by_template({"playerID": "weitao98"}, {"playerID": "weitao99"})
    except Exception as err:
        print("Invalid update, causing duplicate pk: ", err)
    else:
        print("updated:", val.values())

    resp = tbl.delete_by_template({"teamID": "xxx"})
    print("Num. of rows deleted: ", resp)
    print("******************** " + "end  multi pk" + " ********************")


def t2_update_pk(tbl: BaseDataTable):
    print("\n")
    print("******************** " + "test update pk" + " ********************")
    new_values = [{"yearID": "1561", "teamID": "xxx", "lgID": "QQ", "playerID": "weitao99"},
                  {"yearID": "1561", "teamID": "xxx", "lgID": "QQ", "playerID": "weitao98"},
                  {"yearID": "1560", "teamID": "xxx", "lgID": "QQ", "playerID": "weitao99"}]

    for val in new_values:
        try:
            tbl.insert(val)
        except Exception as err:
            print("Invalid insert, causing duplicate pk: ", err)
        else:
            print("inserted:", val.values())

    result1 = tbl.find_by_template({"teamID": "xxx"},
                                   ['yearID', 'teamID', 'lgID', 'playerID', 'G_all'])
    tbl.update_by_key(['1561', 'xxx', 'QQ', 'weitao98'], {"lgID": "TT"})
    result2 = tbl.find_by_template({"teamID": "xxx"},
                                   ['yearID', 'teamID', 'lgID', 'playerID', 'G_all'])
    print("Records in team 'xxx' after update: ", result2)
    assert result1 != result2

    try:
        tbl.update_by_key(['1560', 'xxx', 'QQ', 'weitao99'], {"yearID": "1561"})
        result3 = tbl.find_by_template({"teamID": "xxx"},
                                       ['yearID', 'teamID', 'lgID', 'playerID', 'G_all'])
        print("result 3:", result3)
    except Exception as err:
        print("Invalid update, causing duplicate pk: ", err)
    else:
        print("updated:", val.values())

    resp = tbl.delete_by_key(['1561', 'xxx', 'TT', 'weitao98'])
    print("Num. of rows deleted (by key): ", resp)
    resp = tbl.delete_by_template({"teamID": "xxx"})
    print("Num. of rows deleted (by tpl): ", resp)
    print("******************** " + "end  update pk" + " ********************")


t_csv = test_load()
t_rdb = test_connect()
t2_rdb = t2_connect()

test_insert_delete_template(t_csv)
test_helper(t_csv, t_rdb)
test_query_template(t_csv, t_rdb)
test_query_pk(t_csv, t_rdb)

test_insert_duplicate(t_csv, t_rdb)
test_insert_delete_template(t_csv)
test_insert_delete_template(t_rdb)
test_insert_delete_pk(t_csv)
test_insert_delete_pk(t_rdb)
test_update_tpl(t_rdb)
test_update_tpl(t_csv)
test_update_pk(t_csv)

t2_multi_pk(t2_rdb)
t2_update_pk(t2_rdb)
