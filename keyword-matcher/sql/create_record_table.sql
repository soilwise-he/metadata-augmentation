-- Table: public.records

DROP TABLE IF EXISTS public.records;

CREATE TABLE IF NOT EXISTS public.records
(
    identifier text COLLATE pg_catalog."default" NOT NULL,
    typename text COLLATE pg_catalog."default",
    schema text COLLATE pg_catalog."default",
    mdsource text COLLATE pg_catalog."default",
    insert_date text COLLATE pg_catalog."default",
    xml character varying COLLATE pg_catalog."default",
    anytext text COLLATE pg_catalog."default",
    metadata character varying COLLATE pg_catalog."default",
    metadata_type text COLLATE pg_catalog."default",
    language text COLLATE pg_catalog."default",
    type text COLLATE pg_catalog."default",
    title text COLLATE pg_catalog."default",
    title_alternate text COLLATE pg_catalog."default",
    abstract text COLLATE pg_catalog."default",
    edition text COLLATE pg_catalog."default",
    keywords text COLLATE pg_catalog."default",
    keywordstype text COLLATE pg_catalog."default",
    themes text COLLATE pg_catalog."default",
    parentidentifier text COLLATE pg_catalog."default",
    relation text COLLATE pg_catalog."default",
    time_begin text COLLATE pg_catalog."default",
    time_end text COLLATE pg_catalog."default",
    topicategory text COLLATE pg_catalog."default",
    resourcelanguage text COLLATE pg_catalog."default",
    creator text COLLATE pg_catalog."default",
    publisher text COLLATE pg_catalog."default",
    contributor text COLLATE pg_catalog."default",
    organization text COLLATE pg_catalog."default",
    securityconstraints text COLLATE pg_catalog."default",
    accessconstraints text COLLATE pg_catalog."default",
    otherconstraints text COLLATE pg_catalog."default",
    date text COLLATE pg_catalog."default",
    date_revision text COLLATE pg_catalog."default",
    date_creation text COLLATE pg_catalog."default",
    date_publication text COLLATE pg_catalog."default",
    date_modified text COLLATE pg_catalog."default",
    format text COLLATE pg_catalog."default",
    source text COLLATE pg_catalog."default",
    crs text COLLATE pg_catalog."default",
    geodescode text COLLATE pg_catalog."default",
    denominator text COLLATE pg_catalog."default",
    distancevalue text COLLATE pg_catalog."default",
    distanceuom text COLLATE pg_catalog."default",
    wkt_geometry text COLLATE pg_catalog."default",
    servicetype text COLLATE pg_catalog."default",
    servicetypeversion text COLLATE pg_catalog."default",
    operation text COLLATE pg_catalog."default",
    couplingtype text COLLATE pg_catalog."default",
    operateson text COLLATE pg_catalog."default",
    operatesonidentifier text COLLATE pg_catalog."default",
    operatesoname text COLLATE pg_catalog."default",
    degree text COLLATE pg_catalog."default",
    classification text COLLATE pg_catalog."default",
    conditionapplyingtoaccessanduse text COLLATE pg_catalog."default",
    lineage text COLLATE pg_catalog."default",
    responsiblepartyrole text COLLATE pg_catalog."default",
    specificationtitle text COLLATE pg_catalog."default",
    specificationdate text COLLATE pg_catalog."default",
    specificationdatetype text COLLATE pg_catalog."default",
    platform text COLLATE pg_catalog."default",
    instrument text COLLATE pg_catalog."default",
    sensortype text COLLATE pg_catalog."default",
    cloudcover text COLLATE pg_catalog."default",
    bands text COLLATE pg_catalog."default",
    links text COLLATE pg_catalog."default",
    contacts text COLLATE pg_catalog."default",
    anytext_tsvector tsvector,
    wkb_geometry geometry(Geometry,4326),
    soil_functions text COLLATE pg_catalog."default",
    soil_physical_properties text COLLATE pg_catalog."default",
    productivity text COLLATE pg_catalog."default",
    soil_services text COLLATE pg_catalog."default",
    soil_classification text COLLATE pg_catalog."default",
    soil_processes text COLLATE pg_catalog."default",
    soil_biological_properties text COLLATE pg_catalog."default",
    contamination text COLLATE pg_catalog."default",
    soil_properties text COLLATE pg_catalog."default",
    soil_threats text COLLATE pg_catalog."default",
    ecosystem_services text COLLATE pg_catalog."default",
    soil_chemical_properties text COLLATE pg_catalog."default",
    CONSTRAINT records_pkey PRIMARY KEY (identifier)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.records
    OWNER to soilwise;


-- Table: public.keyword-temp

DROP TABLE IF EXISTS public.keywords_temp;

CREATE TABLE IF NOT EXISTS public.keywords_temp
(
    identifier text COLLATE pg_catalog."default",
    soil_properties text COLLATE pg_catalog."default",
    soil_physical_properties text COLLATE pg_catalog."default",
    soil_processes text COLLATE pg_catalog."default",
    soil_biological_properties text COLLATE pg_catalog."default",
    ecosystem_services text COLLATE pg_catalog."default",
    soil_services text COLLATE pg_catalog."default",
    soil_threats text COLLATE pg_catalog."default",
    soil_chemical_properties text COLLATE pg_catalog."default",
    soil_classification text COLLATE pg_catalog."default",
    productivity text COLLATE pg_catalog."default",
    contamination text COLLATE pg_catalog."default",
    soil_functions text COLLATE pg_catalog."default"
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.keywords_temp
    OWNER to soilwise;