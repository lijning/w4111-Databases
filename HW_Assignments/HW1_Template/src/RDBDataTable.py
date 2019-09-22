from src.BaseDataTable import BaseDataTable
import pymysql, json, logging


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
            for k, v in template:
                conditions.append("%s=%s" % (k, v))
            clause = " and ".join(conditions)
        return clause

    @staticmethod
    def format_field_list(field_list: list):
        if field_list is None:
            return " * "
        else:
            return " %s " % ",".join(field_list)


class RDBDataTable(BaseDataTable):
    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    def __init__(self, table_name, connect_info, key_columns, valid_pk=False):
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
        cur = self._connection.cursor()
        cur.execute("describe " + self._table_name)
        tbl_description = cur.fetchall()
        cur.close()

        self._col_info = {col_info["Field"]: col_info for col_info in tbl_description}

        self._logger.debug("describe table:\n" + json.dump(tbl_description, indent=2))

        # Get the list of key columns for this table.
        if key_columns is None or len(key_columns) == 0:
            self._key_columns = [k for k, v in self._col_info.items() if v["Key"] == "PRI"]
        elif all(map(lambda kvp: kvp["Key"] != "PRI", tbl_description)):
            self._key_columns = key_columns
        else:
            for key in key_columns:
                if self._col_info[key]["Key"] != "PRI":
                    raise Exception("Given incorrect 'key_column'")
            self._key_columns = key_columns

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """
        if len(self._key_columns) != len(key_fields):
            raise Exception("This table has %s key columns but %s values are passed in." % (
                len(self._key_columns), len(key_fields)
            ))
        else:
            template = dict(zip(self._key_columns, key_fields))
            return self.find_by_template(template, field_list)

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
        cur = self._connection.cursor()
        sql = "select {fields} from {table} where {where}".format(
            fields=SqlHelper.format_field_list(field_list),
            table=self._table_name,
            where=SqlHelper.format_where_clause(template)
        )
        self._logger.debug("%s rows are affected." % cur.execute(sql))
        result = cur.fetchall()
        cur.close()
        return result

    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        pass

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        pass

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """

    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """
        pass

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        pass

    def get_rows(self):
        return self._rows

    def get_key_fields_list(self):
        return self._key_columns

    def get_fieldnames_list(self):
        return self._col_info.keys()
