#!/bin/sh

psql "$@" <<EOF
    CREATE FUNCTION drop_all ()
        RETURNS VOID AS
    \$$
    DECLARE rec RECORD;
    BEGIN
        -- Get all the schemas
        FOR rec IN
            select nspname
            from pg_catalog.pg_namespace
	    where nspname != 'information_schema' and nspname not like 'pg_%'
        LOOP
            EXECUTE 'DROP SCHEMA "' || rec.nspname || '" CASCADE';
        END LOOP;
    RETURN;
    END;
    \$$ LANGUAGE plpgsql;

    select drop_all();

    create schema "public";
EOF
