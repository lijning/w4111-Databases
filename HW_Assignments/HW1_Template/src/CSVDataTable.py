from src.BaseDataTable import BaseDataTable, DataTableError, DuplicatedPKeyError
import copy
import csv
import logging
import json
import os
import pandas as pd

pd.set_option("display.width", 256)
pd.set_option('display.max_columns', 20)


class CSVDataTable(BaseDataTable):
    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    _rows_to_print = 10
    _no_of_separators = 2

    def __init__(self, table_name, connect_info, key_columns, debug=True, load=True, rows=None):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns,
            "debug": debug
        }
        self._fieldnames = None

        self._logger = logging.getLogger()

        # self._logger.debug("CSVDataTable.__init__: data = " + json.dumps(self._data, indent=2))
        # TODO commented, not used in test cases after t_load.

        if rows is not None:
            self._rows = copy.copy(rows)
        else:
            self._rows = []
            self._load()

        self._init_key_fields()

    def __str__(self):

        result = "CSVDataTable: config data = \n" + json.dumps(self._data, indent=2)

        no_rows = len(self._rows)
        if no_rows <= CSVDataTable._rows_to_print:
            rows_to_print = self._rows[0:no_rows]
        else:
            temp_r = int(CSVDataTable._rows_to_print / 2)
            rows_to_print = self._rows[0:temp_r]
            keys = self._rows[0].keys()

            for i in range(0, CSVDataTable._no_of_separators):
                tmp_row = {}
                for k in keys:
                    tmp_row[k] = "***"
                rows_to_print.append(tmp_row)

            rows_to_print.extend(self._rows[int(-1 * temp_r) - 1:-1])

        df = pd.DataFrame(rows_to_print)
        result += "\nSome Rows: = \n" + str(df)

        return result

    def _add_row(self, r):
        if self._rows is None:
            self._rows = []
        self._rows.append(r)

    def _load(self):

        dir_info = self._data["connect_info"].get("directory")
        file_n = self._data["connect_info"].get("file_name")
        full_name = os.path.join(dir_info, file_n)

        with open(full_name, "r") as txt_file:
            csv_d_rdr = csv.DictReader(txt_file)
            for r in csv_d_rdr:
                self._add_row(r)

            self._fieldnames = csv_d_rdr.fieldnames

        self._logger.debug("CSVDataTable._load: Loaded " + str(len(self._rows)) + " rows")

    def _init_key_fields(self):
        if self._data.get('key_columns') is None:
            self._data['key_columns'] = self.get_fieldnames_list()[0:1]
        key_fields_val = [[kvp[col] for col in self._data['key_columns']] for kvp in self.get_rows()]
        # if len(key_fields_val) > len(set(key_fields_val)) # You can't set() a list. It's unhashable.
        # TODO more exactly, duplication should be detected?

    def save(self):
        """
        Write the information back to a file.
        :return: None
        """

        dir_info = self._data["connect_info"].get("directory")
        file_n = self._data["connect_info"].get("file_name")
        full_name = os.path.join(dir_info, file_n)

        with open(full_name, "w") as txt_file:
            # csv_d_wtr = csv.DictWriter(txt_file, self._fieldnames)
            if len(self._rows) > 1:
                pass
                # TODO headers.
            csv_d_wtr = csv.DictWriter(txt_file, self.get_fieldnames_list())
            csv_d_wtr.writeheader()
            csv_d_wtr.writerows(self._rows)
            # TODO Blank lines among data when written in this way.

        self._logger.debug("CSVDataTable.save: Saved")

    def get_fieldnames_list(self):
        if self._fieldnames is None:
            if len(self._rows) > 0:
                first_row_keys = self._rows[0].keys()
                self._fieldnames = list(first_row_keys)
        return self._fieldnames

    def get_key_fields_list(self):

        return self._data.get('key_columns')

    @staticmethod
    def matches_template(row, template):

        result = True
        if template is not None:
            for k, v in template.items():
                if v != row.get(k, None):
                    result = False
                    break

        return result

    @staticmethod
    def filter_fields(row: dict, fieldnames: list):
        if fieldnames is None:
            return row
        else:
            return {key: row.get(key) for key in fieldnames}

    @staticmethod
    def update_row(row: dict, new_values: dict):
        for k, v in new_values.items():
            if k in row.keys():
                row[k] = v
            else:
                raise DataTableError("columns %s not found in table." % k)
        return row

    def template_from_key_fields(self, key_fields):
        key_columns = self.get_key_fields_list()
        if len(key_fields) != len(key_columns):
            raise ValueError("%s key fields expected, but only %s passed in."
                             % (len(key_columns), len(key_fields)))
        template_list = zip(key_columns, key_fields)
        template_dict = dict(template_list)
        return template_dict

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """
        template_dict = self.template_from_key_fields(key_fields)
        result = self.find_by_template(template_dict, field_list)
        if len(result) == 0:
            return None
        elif len(result) > 1:
            raise DuplicatedPKeyError
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
        return [self.filter_fields(r, field_list)
                for r in self.get_rows() if self.matches_template(r, template)]

    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :return: A count of the rows deleted.
        """
        template_dict = self.template_from_key_fields(key_fields)
        return self.delete_by_template(template_dict)

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        changed = [row for row in self.get_rows()
                   if not self.matches_template(row, template)]
        num_deleted = len(self.get_rows()) - len(changed)
        self._rows = changed
        return num_deleted

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """
        if any(map(lambda key: key not in self.get_fieldnames_list(), new_values.keys())):
            raise ValueError("Invalid record, wrong field names.")
        target_row = self.find_by_primary_key(key_fields)
        updated = self.update_row(target_row, new_values)
        pk_values = [updated[key] for key in self.get_key_fields_list()]
        if self.find_by_primary_key(pk_values) is None:
            self.delete_by_key(key_fields)
            self._add_row(updated)
        else:
            raise DataTableError("Duplicated primary key after updates: {}".format(pk_values))

    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields. A dictionary containing fields and the values to set
            for the corresponding fields in the records. This returns an error if the update would create a
            duplicate primary key. NO ROWS are updated on this error.
        :return: Number of rows updated.
        """
        if any(map(lambda key: key not in self.get_fieldnames_list(), new_values.keys())):
            raise ValueError("Invalid record, wrong field names.")
        matched_rows = self.find_by_template(template)
        updated_rows = [self.update_row(row, new_values) for row in matched_rows]
        for row in updated_rows:
            pk_values = [row[key] for key in self.get_key_fields_list()]
            try:
                self.find_by_primary_key(pk_values)
            except DuplicatedPKeyError as err:
                err.message = "Duplicated primary key after updates: {}".format(pk_values)
                raise err
        num = self.delete_by_template(template)
        self._logger.debug("{} rows updated.".format(num))
        for row in updated_rows:
            self._add_row(row)
        return num

    def insert(self, new_record: dict):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        if type(new_record) is not dict:
            raise TypeError(new_record)
        elif any(map(lambda key: key not in self.get_fieldnames_list(), new_record.keys())):
            raise ValueError("Invalid record, wrong columns.")
        else:
            pk_values = [new_record[key] for key in self.get_key_fields_list()]
            if self.find_by_primary_key(pk_values) is None:
                self._add_row(new_record)
            else:
                raise DataTableError("Duplicate entry for primary key.")

    def get_rows(self):
        return self._rows
