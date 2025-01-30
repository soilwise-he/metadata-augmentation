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