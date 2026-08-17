"""Retrieve-and-rerank: bi-encoder shortlist -> cross-encoder rerank -> SoilVoc concepts.

Stage 1  chunk the record text, embed each chunk, take the top-k nearest SoilVoc labels
         per chunk (cosine).  Retrieval proposes only; it cannot rank.
Stage 2  cross-encode every retrieved (label, chunk) pair, max-pool to one score per
         concept, drop anything below --min-ce.

The label side is **English** regardless of the record's language, so non-English records
are scored cross-lingually.  This is the configuration explored in ce_testing.ipynb; the
unreviewed `mt-deepl` labels are not involved.

Run:  ../../.venv/bin/python ce/retrieve_rerank.py
      ../../.venv/bin/python ce/retrieve_rerank.py --input ../test_records.json
"""
import argparse, csv, hashlib, json, sys, time
from pathlib import Path

import numpy as np
import torch
from sentence_transformers import CrossEncoder, SentenceTransformer

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent

BI_MODEL = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
CE_MODEL = "cross-encoder/mmarco-mMiniLMv2-L12-H384-v1"
METHOD = "ce_retrieve_rerank"

# Models needing an instruction prefix: (label side, record side). MiniLM needs none.
PREFIXES = {
    "intfloat/multilingual-e5-small": ("passage: ", "query: "),
    "intfloat/multilingual-e5-base": ("passage: ", "query: "),
    "nomic-ai/nomic-embed-text-v2-moe": ("search_document: ", "search_query: "),
}


# --------------------------------------------------------------------- vocab
def load_vocab(path, lang="en"):
    """-> (labels, concept_ids, concept_uris); one entry per label, a concept may have several."""
    with open(path, encoding="utf-8") as f:
        concepts = json.load(f)
    labels, ids, uris = [], [], []
    for c in concepts:
        for lab in c["labels"].get(lang, []):
            labels.append(lab)
            ids.append(c["identifier"].split("#")[-1])
            uris.append(c["identifier"])
    print(f"{len(labels)} {lang} labels from {len(concepts)} concepts")
    return labels, ids, uris


def label_embeddings(bi, labels, model_name, cache_dir):
    """Encode the vocabulary once; cache keyed by a hash of the labels + model name."""
    key = hashlib.sha1(("|".join(labels) + model_name).encode("utf-8")).hexdigest()[:16]
    cache_dir.mkdir(exist_ok=True)
    cache = cache_dir / f"labels_{key}.npz"
    if cache.exists():
        print(f"label embeddings from cache {cache.name}")
        return np.load(cache)["emb"]
    lab_pre, _ = PREFIXES.get(model_name, ("", ""))
    print(f"encoding {len(labels)} labels with {model_name} …")
    emb = bi.encode([lab_pre + l for l in labels], batch_size=128,
                    normalize_embeddings=True, show_progress_bar=True)
    np.savez_compressed(cache, emb=emb)
    return emb


# ------------------------------------------------------------------ chunking
import re

def chunk_text(text, lo=200, hi=900):
    """Sentence-split, then merge back into blocks of roughly lo..hi characters.

    `lo` is the real knob: a chunk is flushed as soon as it passes lo, so chunks land at
    lo..lo+one sentence. Keeps each chunk inside the bi-encoder's 128-token limit.
    """
    text = re.sub(r"\s+", " ", text).strip()
    chunks, buf = [], ""
    for sent in re.split(r"(?<=[.!?])\s+(?=[A-ZÄÖÜ])", text):
        if buf and len(buf) + len(sent) + 1 > hi:
            chunks.append(buf)
            buf = sent
        else:
            buf = f"{buf} {sent}".strip()
        if len(buf) >= lo:
            chunks.append(buf)
            buf = ""
    if buf:
        if chunks and len(buf) < lo:
            chunks[-1] += " " + buf
        else:
            chunks.append(buf)
    return chunks


# ------------------------------------------------------------------ language
def record_lang(r):
    """Language comes from the input JSON, never detected here.

    `lang_abstract` is what make_random_sample.py writes; `lang` is the field in the
    curated test_records.json.
    """
    return r.get("lang_abstract") or r.get("lang") or "unk"


def clean_cell(v):
    return "" if v is None or str(v).strip().upper() in ("", "NULL") else str(v).strip()


# ---------------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--input", default=str(ROOT / "test_records_random.json"),
                    help="test-set JSON with a 'records' list (default: the random sample)")
    ap.add_argument("--vocab", default=str(ROOT / "concepts_multilingual.json"))
    ap.add_argument("--label-lang", default="en", help="vocabulary language (default en)")
    ap.add_argument("--text", choices=["title+abstract", "abstract"], default="title+abstract",
                    help="what to feed the pipeline (default title+abstract)")
    ap.add_argument("--lo", type=int, default=200)
    ap.add_argument("--hi", type=int, default=900)
    ap.add_argument("--top-k", type=int, default=10, help="labels retrieved per chunk")
    ap.add_argument("--min-ce", type=float, default=0.1, help="drop pairs scoring below this")
    ap.add_argument("--ce-max-length", type=int, default=256)
    ap.add_argument("--batch-size", type=int, default=64)
    ap.add_argument("--out", default=None, help="results CSV (default derived from --input)")
    args = ap.parse_args()

    t0 = time.time()
    stem = Path(args.input).stem
    out_path = Path(args.out) if args.out else HERE / f"keywords_{stem}.csv"
    cand_path = HERE / f"candidates_{stem}.csv"

    labels, concept_ids, concept_uris = load_vocab(args.vocab, args.label_lang)

    with open(args.input, encoding="utf-8") as f:
        payload = json.load(f)
    records = payload["records"] if isinstance(payload, dict) else payload
    print(f"{len(records)} records from {Path(args.input).name}")

    print(f"loading bi-encoder {BI_MODEL} …")
    bi = SentenceTransformer(BI_MODEL)
    emb = label_embeddings(bi, labels, BI_MODEL, HERE / ".cache")

    # ---------------- stage 1: chunk + retrieve -----------------------------
    _, txt_pre = PREFIXES.get(BI_MODEL, ("", ""))
    pairs = []          # (rec_idx, chunk_idx, label_idx, cos)
    chunk_store = {}    # rec_idx -> [chunk, ...]
    for ri, r in enumerate(records):
        title, abstract = clean_cell(r.get("title")), clean_cell(r.get("abstract"))
        doc = f"{title}. {abstract}" if args.text == "title+abstract" and title else abstract
        chunks = chunk_text(doc, args.lo, args.hi)
        chunk_store[ri] = chunks
        if not chunks:
            continue
        cos = bi.encode([txt_pre + c for c in chunks], normalize_embeddings=True,
                        batch_size=32) @ emb.T
        k = min(args.top_k, len(labels) - 1)
        for ci in range(len(chunks)):
            for li in np.argpartition(-cos[ci], k)[:k]:
                pairs.append((ri, ci, int(li), float(cos[ci, int(li)])))
    n_chunks = sum(len(c) for c in chunk_store.values())
    print(f"stage 1: {n_chunks} chunks -> {len(pairs)} candidate pairs "
          f"({len({p[2] for p in pairs})} distinct labels reached the CE)")

    # ---------------- stage 2: one batched CE pass --------------------------
    print(f"loading cross-encoder {CE_MODEL} (max_length={args.ce_max_length}) …")
    ce = CrossEncoder(CE_MODEL, activation_fn=torch.nn.Sigmoid(),
                      max_length=args.ce_max_length)
    ce_inputs = [(labels[li], chunk_store[ri][ci]) for ri, ci, li, _ in pairs]
    scores = ce.predict(ce_inputs, batch_size=args.batch_size, show_progress_bar=True)

    # ---------------- max-pool per concept, threshold, write ----------------
    per_record = {ri: {} for ri in range(len(records))}
    with open(cand_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["record_identifier", "chunk", "concept_identifier", "concept_uri",
                    "concept_label", "cos", "ce", "method"])
        for (ri, ci, li, cos), s in zip(pairs, scores):
            s = float(s)
            w.writerow([records[ri].get("identifier", ""), ci, concept_ids[li],
                        concept_uris[li], labels[li], f"{cos:.4f}", f"{s:.4f}", METHOD])
            cid = concept_ids[li]
            best = per_record[ri].get(cid)
            if best is None or s > best[0]:
                per_record[ri][cid] = (s, labels[li])

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["identifier", "abstract", "language", "keywords"])
        kept_total = 0
        for ri, r in enumerate(records):
            abstract = clean_cell(r.get("abstract"))
            kept = sorted((v for v in per_record[ri].values() if v[0] >= args.min_ce),
                          key=lambda v: -v[0])
            kept_total += len(kept)
            w.writerow([r.get("identifier", ""), abstract, record_lang(r),
                        "; ".join(lab for _, lab in kept)])

    n_empty = sum(1 for ri in range(len(records))
                  if not [v for v in per_record[ri].values() if v[0] >= args.min_ce])
    print(f"\nstage 2: {len(pairs)} pairs scored, {kept_total} concepts kept "
          f"(ce >= {args.min_ce}), {kept_total/len(records):.1f} per record")
    print(f"         {n_empty}/{len(records)} records returned no concept at all")
    print(f"results    -> {out_path.relative_to(ROOT)}")
    print(f"candidates -> {cand_path.relative_to(ROOT)}  (all {len(pairs)} pairs, pre-threshold)")
    print(f"total {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
