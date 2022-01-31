# db-pump

A PL/SQL package for moving data from source to target table(s)

The package facilitates a simple pattern for high performance data manipulation based on a source query. It generates and executes an anonymous PL/SQL block that uses a cursor to iterate through the source query and bind the result set to 1 or more target DML statements.

A note on performance - Bulk collection and binding are used to optimize performance. However, using straight SQL will almost always be the fastest approach to accomplish a given task (insert from select statement, etc).

## Installation

Execute [db_pump.sql](DB_PUMP.sql) to create the db_pump package

## Usage
Example schema:
```
CREATE TABLE source_table (s1 NUMBER, s2 VARCHAR2(10), s3 DATE);
CREATE TABLE target_table1 (d1 NUMBER, d2 VARCHAR2(10), d3 DATE, d4 NUMBER);
CREATE TABLE target_table2 (d1 NUMBER, d2 VARCHAR2(10), d3 DATE);

INSERT INTO source_table VALUES (10, 'hello', sysdate);
INSERT INTO source_table VALUES (20, 'world', sysdate);
INSERT INTO source_table VALUES (30, '!', sysdate);

INSERT INTO target_table2 VALUES (10, 'some', sysdate - 1);
INSERT INTO target_table2 VALUES (20, 'other', sysdate - 1);
INSERT INTO target_table2 VALUES (30, 'values', sysdate - 1);


```
1. Set source SQL statement
```
exec db_pump.set_source('SELECT rownum rn, s.* FROM source_table s');
```
  
2. Set 1 to n target SQL statements
```
exec db_pump.add_target('INSERT INTO target_table1 (d1, d2, d3, d4) VALUES (:s1, :s2, :s3, :rn)');
exec db_pump.add_target('UPDATE target_table2 SET d2 = :s2, d3 = :s3 WHERE d1 = :s1');
```

3. Execute the data pump
```
exec db_pump.pump();


--- Example results ---

SELECT * FROM target_table1;

        D1 D2         D3                D4
---------- ---------- --------- ----------
        10 hello      25-JAN-22          1
        20 world      25-JAN-22          2
        30 !          25-JAN-22          3
		
SELECT * FROM target_table2;

        D1 D2         D3
---------- ---------- ---------
        10 hello      25-JAN-22
        20 world      25-JAN-22
        30 !          25-JAN-22
```

## Options
- Reset the target SQL statements. A PL/SQL package is a singleton on a given session, so targets must be reset if there was prior use. The following will remove all targets
```
exec db_pump.reset_targets();
```

- Set the batch size for bulk collect. Default is 1000
```
exec db_pump.set_batch_size(100); 
```

- Set to commit after each target batch is executed. Default is false
```
exec db_pump.set_batch_commit(true); 
```

- Retrieve rowcounts from source and target statements. The db_pump.pump procedure has an overload with OUT parameters for source count (NUMBER) and counts for each target (array of NUMBER)
```
declare 
  source_count number;
  target_counts db_pump.number_table_t;
begin
  db_pump.set_source('...');
  db_pump.add_target('...');
  db_pump.add_target('...');
  db_pump.pump(source_count, target_counts);
  
  dbms_output.put_line('source count = ' || source_count);
  for i in 1..target_counts.count loop
    dbms_output.put_line('target count ' || i || ' = ' || target_counts(i));
  end loop;
end;
/
```

- Retrieve the generated PL/SQL block
```
exec dbms_output.put_line(db_pump.get_pump_sql());
```

## Limitations
The target SQL statements are parsed to extract bind variables. The parsing is "quick and dirty" in that it mainly uses a simple regex. The parsing logic will be incorrect if a colon character (:) exists in the target SQL that is not associated with a bind variable. For example, a colon in comments or quoted text would cause failure in parsing logic and incorrect SQL generation.

The total length of the generated anonymous block of SQL must 4000 characters or less.

The following "tags" are replaced in the generated SQL statement. Any occurences in the source or target would cause incorrect SQL generation:
- `<source_sql_tag>`
- `<target_sql_tag>`
- `<sql_tag>`
- `<binds_tag>`
- `<target_count_index_tag>`