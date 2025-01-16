-- View: harvest.item_contain_keyword

-- DROP VIEW harvest.item_contain_keyword;

CREATE OR REPLACE VIEW harvest.item_contain_keyword
 AS
 SELECT items.identifier,
    items.hash,
    items.uri,
    items.turtle
   FROM harvest.items
  WHERE items.turtle ~~ '%dcat:keyword%'::text OR items.turtle ~~ '%dcat:theme%'::text OR items.turtle ~~ '%dcterms:subject%'::text OR items.turtle ~~ '%dct:subject%'::text;

ALTER TABLE harvest.item_contain_keyword
    OWNER TO soilwise;
COMMENT ON VIEW harvest.item_contain_keyword
    IS 'This view is to query records in harvest.items that the turtle contains keywords;