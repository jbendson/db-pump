CREATE OR REPLACE PACKAGE db_pump AS  
  TYPE varchar_table_t IS TABLE OF VARCHAR2(4000) INDEX BY PLS_INTEGER;
  TYPE number_table_t IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
    
  PROCEDURE set_batch_commit (
    v_batch_commit BOOLEAN
  );
  
  PROCEDURE set_batch_size (
    v_batch_size NUMBER
  );
  
  PROCEDURE set_source (
    v_sql VARCHAR2
  );
  
  PROCEDURE add_target (
    v_sql VARCHAR2
  );
  
  PROCEDURE reset_targets;
  
  PROCEDURE pump;
  
  PROCEDURE pump(
    v_source_count  OUT NUMBER,
    v_target_counts OUT number_table_t
  );
  
  FUNCTION get_pump_sql RETURN VARCHAR2;

END;