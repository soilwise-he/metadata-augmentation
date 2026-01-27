# test_harness.py
import spacy
from spacy.pipeline import EntityRuler
import json
import logging
from typing import List, Tuple

# --- Paste your pipeline class below (unchanged) -----------------------------
# Tip: You can keep your class as-is. We'll monkey-patch DB methods afterwards.
import argparse
from datetime import datetime

# ------- BEGIN: Your existing class (copy as-is, except HTML &gt; replaced by >) -----
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class NERAugmentationPipeline:
    def __init__(self, model_path: str, db_config: dict):
        """Initialize the augmentation pipeline"""
        # We'll allow model_path='__builtin__' to use a lightweight pipeline
        if model_path == "__builtin__":
            self.nlp = spacy.blank("en")
            ruler = self.nlp.add_pipe("entity_ruler")
            # Add a few patterns that map to Location_positive
            patterns = [
                {"label": "Location_positive", "pattern": "Brussels"},
                {"label": "Location_positive", "pattern": "Bruxelles"},
                {"label": "Location_positive", "pattern": "Antwerp"},
                {"label": "Location_positive", "pattern": "Antwerpen"},
                {"label": "Location_positive", "pattern": "Belgium"},
                {"label": "Location_positive", "pattern": "Flanders"},
                {"label": "Location_positive", "pattern": "Merksplas"},
            ]
            ruler.add_patterns(patterns)
        else:
            self.nlp = spacy.load(model_path)

        self.db_config = db_config
        self.process_name = "NER-augmentation"
        
    def get_unprocessed_records(self, limit: int = 100, force: bool = False) -> List[Tuple]:
        """Query records that haven't been processed (DB version – will be monkey-patched in the test)"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()
            
            if force:
                query = """
                SELECT h.identifier, h.title, h.abstract
                FROM harvest.items h
                LEFT JOIN metadata.augment_status a 
                    ON h.identifier = a.record_id 
                    AND a.process = %s
                LIMIT %s;
                """
            else:                
                query = """
                SELECT h.identifier, h.title, h.abstract
                FROM harvest.items h
                LEFT JOIN metadata.augment_status a 
                    ON h.identifier = a.record_id 
                    AND a.process = %s
                WHERE a.record_id IS NULL
                LIMIT %s;
                """
            
            cur.execute(query, (self.process_name, limit))
            records = cur.fetchall()
            cur.close()
            conn.close()
            
            logger.info(f"Retrieved {len(records)} unprocessed records")
            return records
            
        except psycopg2.Error as e:
            logger.error(f"Database error retrieving records: {e}")
            return []
    
    def extract_locations(self, text: str) -> List[str]:
        """Extract location entities from text using trained model"""
        if not text:
            return []
        
        try:
            doc = self.nlp(text)
            locations = [(ent.text, ent.start_char, ent.end_char) for ent in doc.ents 
                        if ent.label_ == 'Location_positive']
            return locations
        except Exception as e:
            logger.error(f"Error extracting locations: {e}")
            return []
    
    def save_batch_augmentations(self, augment_rows, status_rows) -> bool:
        """Save batch augmentation results to database (DB version – will be monkey-patched in the test)"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()
            
            if augment_rows:
                execute_values(
                    cur,
                    """INSERT INTO metadata.augments 
                       (record_id, property, value, process) 
                       VALUES %s""",
                    augment_rows
                )
            
            if status_rows:
                execute_values(
                    cur,
                    """INSERT INTO metadata.augment_status 
                       (record_id, status, process) 
                       VALUES %s""",
                    status_rows
                )
            
            conn.commit()
            cur.close()
            conn.close()
            
            logger.info(f"Saved batch augmentations for {len(status_rows)} records")
            return True
            
        except psycopg2.Error as e:
            logger.error(f"Database error saving batch augmentations: {e}")
            return False

    def save_augmentations(self, record_id: str, augmentations: dict) -> bool:
        """Save augmentation results to database (DB version – will be monkey-patched in the test)"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()
            
            # Insert augmentations
            augment_rows = [
                (record_id, property_name, value, self.process_name)
                for property_name, value in augmentations.items()
                if value
            ]
            
            if augment_rows:
                execute_values(
                    cur,
                    """INSERT INTO metadata.augments 
                       (record_id, property, value, process) 
                       VALUES %s""",
                    augment_rows
                )
            
            # Update augment_status
            cur.execute(
                """INSERT INTO metadata.augment_status 
                   (record_id, status, process) 
                   VALUES (%s, %s, %s)""",
                (record_id, 'processed', self.process_name)
            )
            
            conn.commit()
            cur.close()
            conn.close()
            
            logger.info(f"Saved augmentations for record: {record_id}")
            return True
            
        except psycopg2.Error as e:
            logger.error(f"Database error saving augmentations: {e}")
            return False
    
    def process_batch(self, force: bool = False) -> int:
        """Process a batch of records"""
        if force:
            logger.info("Force flag set - will reprocess all records")
        
        records = self.get_unprocessed_records(limit=100, force=force)
        processed_count = 0
        
        for record_id, title, abstract in records:
            try:
                augmentations = {}
                
                # Extract from title
                if title:
                    locations_title = self.extract_locations(title)
                    if locations_title:
                        augmentations['title'] = json.dumps([
                            {'text': ent[0], 'start_char': ent[1], 'end_char': ent[2]} 
                            for ent in locations_title
                        ])
                
                # Extract from abstract
                if abstract:
                    locations_abstract = self.extract_locations(abstract)
                    if locations_abstract:
                        augmentations['abstract'] = json.dumps([
                            {'text': ent[0], 'start_char': ent[1], 'end_char': ent[2]} 
                            for ent in locations_abstract
                        ])
                
                # Save results
                if self.save_augmentations(record_id, augmentations):
                    processed_count += 1
                    
            except Exception as e:
                logger.error(f"Error processing record {record_id}: {e}")
                continue
        
        logger.info(f"Processed {processed_count}/{len(records)} records")
        return processed_count
# ------- END: Your existing class ------------------------------------------------

# ---------------- Test doubles (no DB) ------------------------------------------
class InMemoryStore:
    """A simple in-memory sink to capture what would be written to the DB."""
    def __init__(self):
        self.augments = []       # list of tuples: (record_id, property, value, process)
        self.augment_status = [] # list of tuples: (record_id, status, process)

class DummyDataSource:
    """Provides fake 'unprocessed' records."""
    def __init__(self):
        # (identifier, title, abstract)
        self.records = [
            (
                "rec-001",
                "Soil survey in Brussels and Antwerp",
                "We sampled plots across Flanders; focus on Merksplas and Belgium-wide baseline."
            ),
            (
                "rec-002",
                "Remote sensing over Flanders",
                "No locations here."  # expects no hits
            ),
            (
                "rec-003",
                "Bruxelles pilot: soil moisture",
                "The Antwerpen region shows variance."
            ),
        ]

# ---------------- Monkey-patching helpers ---------------------------------------
def patch_pipeline_for_memory(pipeline: NERAugmentationPipeline, source: DummyDataSource, sink: InMemoryStore):
    """Replace DB-touching methods with in-memory behaviors."""

    def fake_get_unprocessed_records(limit: int = 100, force: bool = False):
        recs = source.records[:limit]
        logger.info(f"[TEST] Returning {len(recs)} dummy records (force={force})")
        return recs

    def fake_save_augmentations(record_id: str, augmentations: dict) -> bool:
        # Simulate writing augments
        augment_rows = [
            (record_id, property_name, value, pipeline.process_name)
            for property_name, value in augmentations.items()
            if value
        ]
        sink.augments.extend(augment_rows)
        # Simulate writing status
        sink.augment_status.append((record_id, "processed", pipeline.process_name))
        logger.info(f"[TEST] Saved in-memory augmentations for {record_id}: {augmentations}")
        return True

    # Apply patches
    pipeline.get_unprocessed_records = fake_get_unprocessed_records  # type: ignore
    pipeline.save_augmentations = fake_save_augmentations            # type: ignore

# ---------------- Main runnable test --------------------------------------------
def main():
    # Create pipeline with lightweight builtin model
    pipeline = NERAugmentationPipeline(model_path="trained_models\\20251204_output\\model-best", db_config={})
    source = DummyDataSource()
    sink = InMemoryStore()

    patch_pipeline_for_memory(pipeline, source, sink)

    processed = pipeline.process_batch(force=False)
    print("\n=== TEST RUN SUMMARY ===")
    print(f"Processed count: {processed}")

    print("\n--- Augments (what would be INSERTed into metadata.augments) ---")
    for row in sink.augments:
        record_id, prop, value, process = row
        # Pretty print JSON values
        try:
            parsed = json.loads(value)
            value_pp = json.dumps(parsed, indent=2)
        except Exception:
            value_pp = value
        print("*************")
        print(f"\nrecord_id={record_id}\nproperty={prop}\nprocess={process}\nvalue=\n{value_pp}")

    print("\n--- Status (what would be INSERTed into metadata.augment_status) ---")
    for row in sink.augment_status:
        print(row)

if __name__ == "__main__":
    main()
