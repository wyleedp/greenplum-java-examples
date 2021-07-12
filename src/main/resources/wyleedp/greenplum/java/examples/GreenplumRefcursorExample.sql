-- DROP FUNCTION public.ref_fun_test()

CREATE OR REPLACE FUNCTION public.ref_fun_test()
RETURNS SETOF refcursor
AS
$$
DECLARE
    ref1 refcursor := 'refcursor_1';
    ref2 refcursor := 'refcursor_2';
    ref3 refcursor := 'refcursor_3';

    v_sql TEXT;
    v_table varchar := 'pg_catalog.pg_tables';
BEGIN
    
    -- 레프커서 1
    OPEN ref1 FOR
        SELECT 'refcursor_1' AS a
              , NO
              , current_timestamp(0)
           FROM pg_catalog.generate_series(1, 10) NO;
    RETURN NEXT ref1;

    -- 레프커서 2
    OPEN ref2 FOR
        SELECT 'refcursor_2' AS a
              , NO
           FROM pg_catalog.generate_series(1, 5) NO;
    RETURN NEXT ref2;

    -- 레프커서 3 - pg_catalog.pg_tables 테이블을 동적쿼리(Dynamic SQL)로 실행
    v_sql := 'select schemaname, tablename from ' || v_table || ' limit 20';
    
    OPEN ref3 FOR EXECUTE v_sql;
    RETURN NEXT ref3;

END
$$ LANGUAGE plpgsql