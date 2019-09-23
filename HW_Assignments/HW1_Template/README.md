# W4111_F19_HW1
## CSV Data Table

- The main part of this class is a list of ordered dictionary, which stores all the data. The keys of all the dictionaries in this list are the same. This list can be used as a relational database, because each dictionary can be seen as a row or a tuple, and all of them complies with one relation. 
- SELECT: I used list comprehensions to select the desired rows and fields.
- INSERT: After verifying the new values, add them in the "rows".
- DELETE: List comprehension is used as a filter.
- UPDATE:
  - First, I get the matched rows out of the table, make a copy of them, and update them out of space.
  - Second, I removed the matched rows from the original table.
  - Then, for each updated rows, check whether its primary key are in conflict with the current table.
  - If there's no conflict, I can safely add the updated rows into the table.
  - Otherwise, just add the untouched copy of the matched rows back into the table.
  - KEY: Make a deep copy of the matched rows.
- Most errors (including duplicated PK) are handled.

## RDB Data Table

- This class can be generally divided into two parts: one connecting the MySQL server and the other format and construct SQL statements.
- Some pymysql errors are simply raised out while some others are handled. For the latter, warning msg are printed out.