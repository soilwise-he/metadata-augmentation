# match the subjects and write the result to metadata.keyword_match

# input: concepts.json, database: metadata.subjects
# output: database: metadata.keyword_match
#
# Cascade (first hit wins), ported from enriched-ce-testing/enriched_ce_match.py:
#   1. URL match      -> subject URI found in a concept's exact/closeMatch URIs.
#   2. Exact label    -> subject label equals a concept label (case-insensitive,
#                        any language).
#   3. Retrieve + rerank:
#        a. bi-encoder top-k nearest concept labels (cosine >= COSINE_FLOOR),
#           deduped to the best label per concept,
#        b. cross-encoder scores (subject_label, concept enriched_text), where
#           enriched_text = "{primary_en_label}: {definition}" or the bare label,
#        c. best candidate accepted if its CE score >= CE_THRESHOLD.
#
# TEMPORARY score convention (the table schema is unchanged): the integer column
# fuzzymatch_score holds
#   100                   for URL and exact label matches,
#   round(ce_score * 100) for cross-encoder matches (0-1 sigmoid -> 0-100),
#   NULL                  for no match.
# Rows written before this change hold thefuzz fuzz.ratio scores (80-100) and
# NULL for URL matches; they are not reprocessed.

import time

import datetime

import os

import sys

sys.path.append('utils')

from database import dbQuery, insertBulkSQL

from dotenv import load_dotenv

import hashlib

import json, csv

import getopt

import logging

from collections import Counter


HERE = os.path.dirname(os.path.abspath(__file__))
CONCEPTS_PATH = os.path.join(HERE, "concepts.json")
EMB_CACHE_PATH = os.path.join(HERE, "concept_embeddings.npz")
# Local log of every processed subject with its method and raw CE score, since
# the table has no column for either. Set to None to disable.
MATCH_LOG_PATH = os.path.join(HERE, "keyword_match_log.csv")

BI_MODEL_NAME = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
CE_MODEL_NAME = "cross-encoder/mmarco-mMiniLMv2-L12-H384-v1"
TOP_K = 15            # nearest concept labels retrieved per subject
COSINE_FLOOR = 0.40   # drop retrieved candidates below this before reranking
CE_THRESHOLD = 0.60   # 0-1 sigmoid score; placeholder, recalibrate on a gold set
CE_MAX_LENGTH = 128   # passages are short; bounds padding cost on CPU
EXACT_SCORE = 100     # fuzzymatch_score for URL / exact label matches


def turple2dict(rows): # transform a query result from turple to dict
    col_names = ['id', 'uri', 'label']
    return [dict(zip(col_names, row)) for row in rows]


def clean_value(value):
    """Treat None, empty strings and literal 'NULL' as missing."""
    if value is None:
        return None
    value = str(value).strip()
    if value == "" or value.upper() == "NULL":
        return None
    return value


def formatString(input_string): # remove prefix numbers from a label
    stripped_string = input_string.lstrip()
    if stripped_string and stripped_string[0].isdigit():
        return stripped_string.lstrip('0123456789').lstrip()
    return stripped_string


def all_labels(concept):
    """Flatten a concept's labels across all languages."""
    return [lab for labels in concept['labels'].values() for lab in labels]


def primary_label(concept):
    """English primary label if present, else the first available label."""
    en = concept['labels'].get('en')
    if en:
        return en[0]
    flat = all_labels(concept)
    return flat[0] if flat else concept['identifier']


def url_match(cons, sub_url):
    """Return the first concept whose URIs contain the subject URL, else None."""
    for c in cons:
        if sub_url in c['uris']:
            return c
    return None


def exact_label_match(cons, key_value):
    """Exact, case-insensitive label match against any language label."""
    key = formatString(key_value).lower()
    if not key:
        return None
    for c in cons:
        for lab in all_labels(c):
            if lab.lower() == key:
                return c
    return None


def build_corpus(cons):
    """Flatten all concept labels into a corpus parallel to concept indices."""
    corpus_labels, corpus_concept_idx = [], []
    for ci, c in enumerate(cons):
        for lab in all_labels(c):
            corpus_labels.append(lab)
            corpus_concept_idx.append(ci)
    return corpus_labels, corpus_concept_idx


def concepts_fingerprint(cons):
    """Stable hash of the concept labels + bi-encoder name (cache key)."""
    h = hashlib.sha256()
    h.update(BI_MODEL_NAME.encode("utf-8"))
    for c in cons:
        h.update(c['identifier'].encode("utf-8"))
        for lab in all_labels(c):
            h.update(lab.encode("utf-8"))
    return h.hexdigest()


def load_or_build_concept_embeddings(bi_model, corpus_labels, fingerprint):
    """Embed concept labels once and cache them, keyed by the fingerprint."""
    import numpy as np
    import torch

    if os.path.exists(EMB_CACHE_PATH):
        cached = np.load(EMB_CACHE_PATH, allow_pickle=True)
        if str(cached['fingerprint']) == fingerprint:
            logging.info(f"Loaded cached concept embeddings ({len(corpus_labels)} labels)")
            return torch.from_numpy(cached['embeddings'])
        logging.info("Embedding cache stale (vocabulary or model changed); rebuilding")

    logging.info(f"Embedding {len(corpus_labels)} concept labels with the bi-encoder")
    emb = bi_model.encode(corpus_labels, convert_to_numpy=True,
                          normalize_embeddings=True, show_progress_bar=False)
    try:
        np.savez(EMB_CACHE_PATH, embeddings=emb, fingerprint=fingerprint)
    except OSError as e:  # read-only deploy dir: keep going without the cache
        logging.warning(f"Could not write embedding cache: {e}")
    return torch.from_numpy(emb)


def ce_to_db_score(ce_score):
    """Map a 0-1 cross-encoder score onto the integer fuzzymatch_score column."""
    return int(round(ce_score * 100))


def match_res_sub(items, cons):
    """
    items: subjects from the database, dicts with id / uri / label
    cons: concepts from concepts.json

    Returns one dict per subject with sub_identifier, vocab_identifier,
    vocab_label, fuzzymatch_score (see the score convention at the top),
    plus method and ce_score for the local log (not written to the table).
    """
    matched_dict_list = []
    retrieval_idx = []     # indices into matched_dict_list needing retrieval
    retrieval_labels = []  # their cleaned labels

    for res in items:
        matched_dict = {
            'sub_identifier': res.get('id'),
            'sub_label': clean_value(res.get('label')),
            'vocab_identifier': None,
            'vocab_label': None,
            'fuzzymatch_score': None,
            'method': 'no_match',
            'ce_score': None,
        }
        matched_dict_list.append(matched_dict)

        sub_uri = clean_value(res.get('uri'))
        sub_label = matched_dict['sub_label']

        # 1. URL match
        if sub_uri is not None:
            matched_con = url_match(cons, sub_uri)
            if matched_con is not None:
                matched_dict.update(vocab_identifier=matched_con['identifier'],
                                    vocab_label=primary_label(matched_con),
                                    fuzzymatch_score=EXACT_SCORE,
                                    method='url_match')
                continue

        if sub_label is None or not formatString(sub_label):
            continue

        # 2. Exact label match
        matched_con = exact_label_match(cons, sub_label)
        if matched_con is not None:
            matched_dict.update(vocab_identifier=matched_con['identifier'],
                                vocab_label=primary_label(matched_con),
                                fuzzymatch_score=EXACT_SCORE,
                                method='exact_match')
            continue

        # 3. Defer to retrieve + rerank (batched below)
        retrieval_idx.append(len(matched_dict_list) - 1)
        retrieval_labels.append(sub_label)

    logging.info(f"Subjects needing retrieval + rerank: {len(retrieval_idx)}")
    if not retrieval_idx:
        return matched_dict_list

    # Heavy imports / model loads only when the semantic stage is needed.
    import torch
    from sentence_transformers import SentenceTransformer, CrossEncoder, util

    corpus_labels, corpus_concept_idx = build_corpus(cons)
    bi_model = SentenceTransformer(BI_MODEL_NAME)
    corpus_emb = load_or_build_concept_embeddings(
        bi_model, corpus_labels, concepts_fingerprint(cons))

    query_emb = bi_model.encode(retrieval_labels, convert_to_tensor=True,
                                normalize_embeddings=True, show_progress_bar=False)
    hits = util.semantic_search(query_emb, corpus_emb, top_k=TOP_K,
                                score_function=util.cos_sim)

    ce_pairs = []      # (subject_label, concept enriched_text)
    ce_pair_meta = []  # (index into matched_dict_list, concept index)
    for q, subject_hits in enumerate(hits):
        best_per_concept = {}  # concept idx -> best cosine
        for hit in subject_hits:
            if hit['score'] < COSINE_FLOOR:
                continue
            cidx = corpus_concept_idx[hit['corpus_id']]
            if hit['score'] > best_per_concept.get(cidx, -1):
                best_per_concept[cidx] = hit['score']
        for cidx in best_per_concept:
            ce_pairs.append((retrieval_labels[q], cons[cidx]['enriched_text']))
            ce_pair_meta.append((retrieval_idx[q], cidx))

    logging.info(f"Cross-encoder pairs to score: {len(ce_pairs)}")
    if not ce_pairs:
        return matched_dict_list

    ce_model = CrossEncoder(CE_MODEL_NAME, activation_fn=torch.nn.Sigmoid(),
                            max_length=CE_MAX_LENGTH)
    scores = ce_model.predict(ce_pairs, show_progress_bar=False)

    best_by_subject = {}  # list index -> (ce_score, concept idx)
    for (li, cidx), score in zip(ce_pair_meta, scores):
        score = float(score)
        if li not in best_by_subject or score > best_by_subject[li][0]:
            best_by_subject[li] = (score, cidx)

    for li, (score, cidx) in best_by_subject.items():
        matched_dict = matched_dict_list[li]
        matched_dict['ce_score'] = round(score, 4)  # kept for the log, even below threshold
        if score >= CE_THRESHOLD:
            concept = cons[cidx]
            matched_dict.update(vocab_identifier=concept['identifier'],
                                vocab_label=primary_label(concept),
                                fuzzymatch_score=ce_to_db_score(score),
                                method='enriched_cross_encoder')

    return matched_dict_list


def write_match_log(matched_data, process_datetime):
    """Append method + raw CE score per subject to a local CSV (not the DB)."""
    if not MATCH_LOG_PATH:
        return
    fields = ['process_time', 'sub_identifier', 'sub_label', 'method',
              'vocab_identifier', 'vocab_label', 'ce_score', 'fuzzymatch_score']
    try:
        new_file = not os.path.exists(MATCH_LOG_PATH)
        with open(MATCH_LOG_PATH, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
            if new_file:
                writer.writeheader()
            for item in matched_data:
                writer.writerow({**item, 'process_time': process_datetime.isoformat()})
    except OSError as e:
        logging.warning(f"Could not write match log: {e}")


def batch_process(interval):

    start_time = time.time()

    load_dotenv()

    logging.info("Querying records from the database view")

    # the query filters out subjects that have been processed
    sql = '''
    SELECT id, uri, label FROM metadata.subjects s
    WHERE NOT EXISTS (
    SELECT 1 FROM metadata.keyword_match km
    WHERE km.subject_id = s.id
    )
    '''
    result_items = turple2dict(dbQuery(sql, hasoutput=True))

    if len(result_items) == 0:
        logging.info("All records have been processed for the keyword-matcher")
        return

    logging.info(f"Query completed, find {len(result_items)} new records")
    logging.info(f"Query execution: {time.time() - start_time:.4f} seconds")

    start_time = time.time()

    with open(CONCEPTS_PATH, "r", encoding="utf-8") as f:
        cons = json.load(f)
    logging.info("concepts.json file loaded")

    logging.info("Matching records with concepts")

    matched_data = match_res_sub(result_items, cons)

    logging.info(f"Match methods: {dict(Counter(item['method'] for item in matched_data))}")
    logging.info(f"Match completed, match execution: {time.time() - start_time:.4f} seconds")
    start_time = time.time()

    process_datetime = datetime.datetime.fromtimestamp(start_time, tz=datetime.timezone.utc)
    write_match_log(matched_data, process_datetime)

    logging.info("Inserting matched data to database table")

    matched_data_tuples = [
        (item['sub_identifier'], item['vocab_identifier'], item['vocab_label'],
         item['fuzzymatch_score'], process_datetime)
        for item in matched_data
    ]
    insertBulkSQL('metadata.keyword_match',
                  ['subject_id', 'vocab_id', 'vocab_label', 'fuzzymatch_score', 'process_time'],
                  matched_data_tuples)

    logging.info(f"Database insert completed, execution: {time.time() - start_time:.4f} seconds")


def main(argv):

    arg_help = "usage: {0} [-t <time>]".format(argv[0])
    arg_time = 0
    opts, args = getopt.getopt(argv[1:], "ht:o:", ["help", "time=", "output="])

    logging.basicConfig(level=logging.INFO, format = '%(message)s')

    for opt, arg in opts:
        if opt in ("-h", "--help"):
            logging.info(arg_help)
            sys.exit()
        elif opt in ("-t", "--time"):
            arg_time = arg

    logging.info("Start batch process running")
    batch_process(arg_time)


if __name__ == "__main__":
    main(sys.argv)
