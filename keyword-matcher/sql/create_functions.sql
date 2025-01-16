-- FUNCTION: harvest.create_dynamic_table(text, text[])

-- DROP FUNCTION IF EXISTS harvest.create_dynamic_table(text, text[]);

CREATE OR REPLACE FUNCTION harvest.create_dynamic_table(
	table_name text,
	column_names text[])
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    column_definitions text;          -- Will hold the column definition string
    create_table_sql text;           -- Will hold the complete CREATE TABLE statement
    column_name text;                -- Used in loop to process each column
BEGIN
    -- Initialize empty column definitions string
    column_definitions := '';
    
    -- Loop through each column name in the input array
    FOREACH column_name IN ARRAY column_names
    LOOP
        -- Add column name and type, with comma if not first column
        IF column_definitions <> '' THEN
            column_definitions := column_definitions || ', ';
        END IF;
        column_definitions := column_definitions || quote_ident(column_name) || ' text';
    END LOOP;
    
    -- Construct the complete CREATE TABLE statement
    create_table_sql := format(
        'CREATE TABLE IF NOT EXISTS %I (%s)',
        table_name,
        column_definitions
    );
    
    -- Execute the CREATE TABLE statement
    EXECUTE create_table_sql;
END;
$BODY$;

ALTER FUNCTION harvest.create_dynamic_table(text, text[])
    OWNER TO soilwise;



-- FUNCTION: harvest.insert_records_byjoin()

DROP FUNCTION IF EXISTS harvest.insert_records_byjoin();

CREATE OR REPLACE FUNCTION harvest.insert_records_byjoin(
	)
    RETURNS void
    LANGUAGE 'sql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
TRUNCATE TABLE public.records;
INSERT INTO public.records
SELECT 
    public.records2.*,  -- All columns from records2
    public.keywords_temp.soil_functions,
	public.keywords_temp.soil_physical_properties,
	public.keywords_temp.productivity,
	public.keywords_temp.soil_services,
	public.keywords_temp.soil_classification,
	public.keywords_temp.soil_processes,
	public.keywords_temp.soil_biological_properties,
	public.keywords_temp.contamination,
	public.keywords_temp.soil_properties,
    public.keywords_temp.soil_threats,
	public.keywords_temp.ecosystem_services,
	public.keywords_temp.soil_chemical_properties
FROM public.records2
LEFT JOIN public.keywords_temp ON public.records2.identifier = public.keywords_temp.identifier;
-- DROP TABLE IF EXISTS public.keywords_temp -- Drop temp table if not needed
$BODY$;

ALTER FUNCTION harvest.insert_records_byjoin()
    OWNER TO soilwise;