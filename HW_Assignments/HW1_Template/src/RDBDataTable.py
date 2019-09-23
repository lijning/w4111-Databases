import logging
import pymysql
import json

from src.BaseDataTable import BaseDataTable, DataTableError


class SqlHelper:
    """
    A helper class for constructing sql statements.
    """

    @staticmethod
    def format_where_clause(template: dict):
        if template is None or template == {}:
            clause = ""
        else:
            conditions = []
            for k, v in template.items():
                if type(v) is str:
                    conditions.append("%s='%s'" % (k, v))
                else:
                    conditions.append("%s=%s" % (k, v))
            clause = " and ".join(conditions)
        return clause

    @staticmethod
    def format_field_list(field_list: list):
        if field_list is None:
            return "*"
        else:
            return "%s" % ",".join(field_list)

    @staticmethod
    def construct_insert(record: dict, table_name):
        keys = record.keys()
        args = record.values()
        sql = "insert into {} ({}) values ({})".format(table_name, ", ".join(keys),
                                                       ", ".join(["%s"] * len(args)))
        return sql, list(args)

    @staticmethod
    def construct_update(set_template: dict, conditions: dict, table_name: str):
        sql = "UPDATE {0} SET {1} WHERE {2};"
        where = SqlHelper.format_where_clause(conditions)
        columns = ", ".join(["{0} = %({0})s".format(key) for key in set_template.keys()])
        return sql.format(table_name, columns, where), set_template


class RDBDataTable(BaseDataTable):
    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    def __init__(self, table_name, connect_info, key_columns):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        for key in ["host", "user", "password", "database"]:
            if key not in connect_info:
                raise ValueError("Invalid connect_info.")
        self._connection = pymysql.connections.Connection(host=connect_info["host"],
                                                          user=connect_info["user"],
                                                          password=connect_info["password"],
                                                          database=connect_info["database"],
                                                          cursorclass=pymysql.cursors.DictCursor)
        self._table_name = table_name
        self._key_columns = key_columns
        self._logger = logging.getLogger()
        cur = self.get_cursor()
        cur.execute("describe " + self._table_name)
        tbl_description = cur.fetchall()
        cur.close()

        self._col_info = {col_info["Field"]: col_info for col_info in tbl_description}

        # self._logger.debug("describe table:\n" + json.dumps(tbl_description, indent=2))

        # Get the list of key columns for this table.
        if key_columns is None or len(key_columns) == 0:
            self._key_columns = [k for k, v in self._col_info.items() if v["Key"] == "PRI"]
        elif all(map(lambda kvp: kvp["Key"] != "PRI", tbl_description)):
            self._key_columns = key_columns
        else:
            for key in key_columns:
                if self._col_info[key]["Key"] != "PRI":
                    # raise DataTableError("Given incorrect 'key_column'")
                    self._logger.warning("Given incorrect 'key_column'")
            self._key_columns = [k for k, v in self._col_info.items() if v["Key"] == "PRI"]

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """
        if len(self._key_columns) != len(key_fields):
            raise DataTableError("This table has %s key columns but %s values are passed in." % (
                len(self._key_columns), len(key_fields)
            ))
        else:
            template = dict(zip(self._key_columns, key_fields))
            result = self.find_by_template(template, field_list)
            if len(result) == 0:
                return None
            elif len(result) > 1:
                raise DataTableError(json.dumps(result, indent=2))
            else:
                return result[0]

    def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None):
        """

        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}
        :param field_list: A list of request fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A list containing dictionaries. A dictionary is in the list representing each record
            that matches the template. The dictionary only contains the requested fields.
        """
        cur = self.get_cursor()
        sql = "select {fields} from {table} where {where};".format(
            fields=SqlHelper.format_field_list(field_list),
            table=self._table_name,
            where=SqlHelper.format_where_clause(template)
        )
        self._logger.debug("# sql statement: %s" % sql)
        response = cur.execute(sql)
        self._logger.debug("%s rows are affected." % response)
        result = cur.fetchall()
        self._connection.commit()
        cur.close()
        if len(result) == 0:
            return []
        else:
            return result

    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :return: A count of the rows deleted.
        """
        if len(self._key_columns) != len(key_fields):
            raise DataTableError("This table has %s key columns but %s values are passed in." % (
                len(self._key_columns), len(key_fields)
            ))
        else:
            template = dict(zip(self._key_columns, key_fields))
            return self.delete_by_template(template)

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        cur = self.get_cursor()
        sql = "delete from {table} where {where};".format(
            table=self._table_name,
            where=SqlHelper.format_where_clause(template)
        )
        self._logger.debug("# sql statement: %s" % sql)
        response = cur.execute(sql)
        self._logger.debug("%s rows are affected." % response)
        self._connection.commit()
        cur.close()
        return response

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """
        if len(self._key_columns) != len(key_fields):
            raise DataTableError("This table has %s key columns but %s values are passed in." % (
                len(self._key_columns), len(key_fields)
            ))
        else:
            template = dict(zip(self._key_columns, key_fields))
            return self.update_by_template(template, new_values)

    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """
        cur = self.get_cursor()
        sql, args = SqlHelper.construct_update(set_template=new_values, conditions=template,
                                               table_name=self._table_name)
        self._logger.debug(cur.mogrify(sql, args))
        response = cur.execute(sql, args)
        self._logger.debug("%s rows were updated." % response)
        self._connection.commit()
        cur.close()
        return response

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        cur = self.get_cursor()
        sql, args = SqlHelper.construct_insert(new_record, self._table_name)
        try:
            response = cur.execute(sql, args)
            self._logger.debug("# insert:%s" % response)
            self._connection.commit()
        except pymysql.err.IntegrityError as ex:
            raise DataTableError(ex) from ex
        return

    def get_key_fields_list(self):
        return self._key_columns

    def get_fieldnames_list(self):
        return self._col_info.keys()

    def get_cursor(self):
        return self._connection.cursor()
