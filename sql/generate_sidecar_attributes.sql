-- generate_sidecar_attributes(table_name, schema_name)
--
-- Generates a YAML `attributes:` block for nats_sidecar config by inspecting
-- the columns of a PostgreSQL table. Type mapping follows pg_zerialize's
-- datum_to_dynamic() dispatch.
--
-- Usage:
--   SELECT generate_sidecar_attributes('sensor_readings');
--   SELECT generate_sidecar_attributes('sensor_readings', 'public');

CREATE OR REPLACE FUNCTION generate_sidecar_attributes(
    p_table_name  text,
    p_schema_name text DEFAULT 'public'
)
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
    rec           record;
    result        text := 'attributes:' || E'\n';
    sidecar_type  text;
    col_type      text;
    col_udt       text;
BEGIN
    FOR rec IN
        SELECT column_name, data_type, udt_name
        FROM information_schema.columns
        WHERE table_name = p_table_name
          AND table_schema = p_schema_name
        ORDER BY ordinal_position
    LOOP
        col_type := rec.data_type;
        col_udt  := rec.udt_name;

        -- Array types: data_type = 'ARRAY', udt_name starts with '_'
        IF col_type = 'ARRAY' THEN
            IF col_udt IN ('_int2', '_int4', '_int8') THEN
                sidecar_type := 'integer_list';
            ELSE
                sidecar_type := 'string_list';
            END IF;
        ELSIF col_type = 'boolean' THEN
            sidecar_type := 'boolean';
        ELSIF col_type IN ('smallint', 'integer', 'bigint') THEN
            sidecar_type := 'integer';
        ELSIF col_type IN ('real', 'double precision', 'numeric') THEN
            sidecar_type := 'float';
        ELSE
            sidecar_type := 'string';
        END IF;

        result := result || '  - name: ' || rec.column_name || E'\n'
                         || '    type: ' || sidecar_type     || E'\n';
    END LOOP;

    RETURN result;
END;
$$;
