CREATE OR REPLACE PACKAGE BODY db_pump AS  
  p_batch_commit BOOLEAN := FALSE;
  p_batch_size   NUMBER := 1000;
  p_source_sql   VARCHAR2(4000);
  p_target_sqls  varchar_table_t;
    
  FUNCTION prep_sql (
    v_sql VARCHAR2
  ) RETURN VARCHAR2
  AS
    prepped_sql VARCHAR2(4000);
  BEGIN
    prepped_sql := trim(v_sql);
    IF substr(prepped_sql, -1) = ';' THEN
      prepped_sql := substr(v_sql, 1, length(v_sql) - 1);
    END IF;
        
    RETURN prepped_sql;
  END;
  
  FUNCTION get_bind_vars (
    v_sql       VARCHAR2,
    v_bind_name VARCHAR2
  ) RETURN varchar_table_t
  AS
    regex       CONSTANT VARCHAR2(100) := ':(\S+)\s?';
    matches     varchar_table_t;
    prepped_sql VARCHAR2(4000);
  BEGIN
    prepped_sql := replace(replace(replace(replace(v_sql,
      chr(13)||chr(10), ' '),
      ',', ' '),
      '(', ' '),
      ')', ' ');
      
    SELECT v_bind_name || '.' || ltrim(trim(val), ':') BULK COLLECT INTO matches
    FROM (
      SELECT
        regexp_substr(prepped_sql, regex, 1, LEVEL) AS val
      FROM dual
      CONNECT BY
        regexp_substr(prepped_sql, regex, 1, level) IS NOT NULL
    ) WHERE val IS NOT NULL;
  
    RETURN matches;
  END;
  
  FUNCTION join_varchar(
    varchar_table varchar_table_t,
    join_by       VARCHAR2) RETURN VARCHAR
  AS
    comma_varchar VARCHAR2(4000);
  BEGIN
    SELECT listagg(column_value, join_by) INTO comma_varchar
    FROM TABLE(varchar_table);
    
    RETURN comma_varchar;
  END;
  
  PROCEDURE set_batch_commit (
    v_batch_commit BOOLEAN
  )
  AS
  BEGIN
    p_batch_commit := v_batch_commit;
  END;
  
  PROCEDURE set_batch_size (
    v_batch_size NUMBER
  )
  AS
  BEGIN
    p_batch_size := v_batch_size;
  END;
  
  PROCEDURE set_source (
    v_sql VARCHAR2
  )
  AS
  BEGIN
    p_source_sql := prep_sql(v_sql);
  END;
    
  PROCEDURE add_target (
    v_sql VARCHAR2
  ) AS
  BEGIN
    p_target_sqls(p_target_sqls.count + 1) := prep_sql(v_sql);
  END;
    
  PROCEDURE reset_targets
  AS
  BEGIN
    p_target_sqls.delete();
  END;
  
  PROCEDURE pump
  AS
    source_count  NUMBER;
    target_counts number_table_t;
  BEGIN
    pump(source_count, target_counts);
  END;
  
  PROCEDURE pump(
    v_source_count  OUT NUMBER,
    v_target_counts OUT number_table_t
  )
  AS
    pump_sql VARCHAR2(4000); 
  BEGIN
    pump_sql := get_pump_sql();
    --dbms_output.put_line(pump_sql);
    EXECUTE IMMEDIATE pump_sql USING p_batch_size, p_batch_commit, OUT v_source_count, OUT v_target_counts;
  END;
  
  FUNCTION get_pump_sql RETURN VARCHAR2
  AS
    pump_sql_template CONSTANT VARCHAR2(4000) := '
      DECLARE
        CURSOR source_c IS <source_sql_tag>;
        TYPE source_row_t IS TABLE OF source_c%ROWTYPE;
        source_rows source_row_t;
        source_count NUMBER := 0;
        target_counts db_pump.number_table_t;
      BEGIN
        OPEN source_c;
        LOOP
          FETCH source_c BULK COLLECT INTO source_rows LIMIT :limit;
          EXIT WHEN source_rows.COUNT = 0;
          source_count := source_count + source_rows.COUNT;
          <target_sql_tag>
          EXIT WHEN source_rows.COUNT < :limit;
        END LOOP;
        :source_count := source_count;
        :target_counts := target_counts;
      END;';
      
    target_sql_template CONSTANT VARCHAR2(4000) := '
          FORALL i in source_rows.FIRST..source_rows.LAST
            EXECUTE IMMEDIATE ''<sql_tag>''
            USING <binds_tag>;
          IF NOT target_counts.exists(<target_count_index_tag>) THEN
            target_counts(<target_count_index_tag>) := 0;
          END IF;
          target_counts(<target_count_index_tag>) := target_counts(<target_count_index_tag>) + SQL%ROWCOUNT;
          IF :batch_commit THEN
            COMMIT;
          END IF;';
          
    target_sql VARCHAR2(4000);
  BEGIN

    FOR i in 1..p_target_sqls.count LOOP
      target_sql := target_sql || replace(replace(replace(target_sql_template,
        '<sql_tag>', p_target_sqls(i)),
        '<binds_tag>', join_varchar(get_bind_vars(p_target_sqls(i), 'source_rows(i)'), ', ')),
        '<target_count_index_tag>', i);
    END LOOP;
    
    RETURN replace(replace(pump_sql_template,
      '<source_sql_tag>', p_source_sql),
      '<target_sql_tag>', target_sql);
  END;
    
END;