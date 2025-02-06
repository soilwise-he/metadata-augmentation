ALTER TABLE public.keywords_temp
ADD CONSTRAINT id_uniq UNIQUE (identifier);

ALTER TABLE harvest.process_tracking
ADD CONSTRAINT hash_uniq UNIQUE (hash);