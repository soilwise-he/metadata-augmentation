import spacy
import psycopg2
from psycopg2.extras import execute_values
import logging
import argparse
from typing import List, Tuple
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class NERAugmentationPipeline:
    def __init__(self, model_path: str, db_config: dict):
        """Initialize the augmentation pipeline"""
        self.nlp = spacy.load(model_path)
        self.db_config = db_config
        self.process_name = "NER-augmentation"
        
    def get_unprocessed_records(self, limit: int = 100) -> List[Tuple]:
        """Query records that haven't been processed"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()
            
               
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
            locations = [(ent.text,ent.start_char, ent.end_char) for ent in doc.ents 
                        if ent.label_ == 'Location_positive']
            return locations
        except Exception as e:
            logger.error(f"Error extracting locations: {e}")
            return []
    
    def save_batch_augmentations(self, augment_rows: List[Tuple[str, str, str, str]],
                                        status_rows: List[Tuple[str, str, str]]
                                        ) -> bool:
        
        """Save batch augmentation results to database"""
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
        """Save augmentation results to database"""
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
    
    def process_batch(self) -> int:
        """Process a batch of records"""
        
        records = self.get_unprocessed_records(limit=100)
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


def main():
    parser = argparse.ArgumentParser(
        description='Augment metadata with location entities'
    )
    parser.add_argument(
        '--model-path',
        required=True,
        help='Path to trained spaCy model'
    )
    parser.add_argument(
        '--host',
        default='localhost',
        help='Database host'
    )
    parser.add_argument(
        '--port',
        default=5432,
        type=int,
        help='Database port'
    )
    parser.add_argument(
        '--database',
        default='soilwise',
        help='Database name'
    )
    parser.add_argument(
        '--user',
        required=True,
        help='Database user'
    )
    parser.add_argument(
        '--password',
        required=True,
        help='Database password'
    )

    
    args = parser.parse_args()
    
    db_config = {
        'host': args.host,
        'port': args.port,
        'database': args.database,
        'user': args.user,
        'password': args.password
    }
    
    pipeline = NERAugmentationPipeline(args.model_path, db_config)
    pipeline.process_batch()


if __name__ == '__main__':
    main()