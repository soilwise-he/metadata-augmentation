# NER Augmentation Pipeline

This repository contains a Python pipeline for augmenting metadata records with Named Entity Recognition (NER) outputs using a trained spaCy model. The pipeline extracts **location** entities from titles and abstracts stored in a PostgreSQL database and writes the augmented information back into metadata tables.

---

## Features

- Extracts `Location_positive` entities using a custom spaCy model  
- Processes metadata in batches  
- Writes augmentations to `metadata.augments`  
- Tracks processing status in `metadata.augment_status`  
- Supports forced reprocessing (`--force`)  
- Uses structured logging for full visibility  

---

## How It Works

1. Load a trained spaCy model.
2. Retrieve unprocessed records from `harvest.items`.
3. Run NER pipeline on `title` and `abstract` fields.
4. Store extracted entities in JSON format in `metadata.augments`.
5. Record processing status in `metadata.augment_status`.

---

## Expected Database Tables

### `harvest.items`
| Column       | Description |
|--------------|-------------|
| identifier   | Primary key |
| title        | Title text  |
| abstract     | Abstract    |

### `metadata.augments`
| Column     | Description                   |
|------------|--------------------------------|
| record_id  | ID of processed record         |
| property   | `title` or `abstract`          |
| value      | JSON list of extracted entities |
| process    | Name of process (`NER-augmentation`) |

### `metadata.augment_status`
| Column     | Description              |
|------------|---------------------------|
| record_id  | Processed record ID       |
| status     | Typically `processed`     |
| process    | Process name              |

---

## Installation (using `uv`)

```bash
uv venv
uv sync
```
---

## Usage
```bash
uv run ner_augment.py 
--model-path trained_models/20251204_output/model-best
--host <db_host> 
--port <db_port> 
--database <db_name> 
--user <db_user> 
--password <db_password>
--force # forces reporcessing already processed items  
```

