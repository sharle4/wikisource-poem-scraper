import json
import gzip
import sys
import logging
from pydantic import ValidationError
from src.scriptorium.schemas import PoemSchema

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def validate_ndjson_file(filepath: str):
    """
    Validates a .jsonl.gz file using the PoemSchema Pydantic schema.
    """
    logger.info(f"Starting validation for: {filepath}")
    total_lines = 0
    valid_records = 0
    errors = 0

    try:
        with gzip.open(filepath, 'rt', encoding='utf-8') as f:
            for line_number, line in enumerate(f, 1):
                total_lines += 1
                line_content = line.strip()
                if not line_content:
                    logger.warning(f"Line {line_number}: Empty line, skipped.")
                    continue

                try:
                    data = json.loads(line_content)
                    PoemSchema.model_validate(data)
                    valid_records += 1

                except json.JSONDecodeError as e:
                    logger.error(f"Line {line_number}: JSON decoding error. Details: {e}")
                    errors += 1
                except ValidationError as e:
                    logger.error(f"Line {line_number}: Schema validation failed.")
                    logger.error(f"  Title: {data.get('title', 'N/A')}, PageID: {data.get('page_id', 'N/A')}")
                    for error in e.errors():
                        logger.error(f"    Field: {'.'.join(map(str, error['loc']))}, Error: {error['msg']}")
                    errors += 1
                except Exception as e:
                    logger.critical(f"Line {line_number}: Unexpected error: {e}")
                    errors += 1

    except FileNotFoundError:
        logger.critical(f"Error: File '{filepath}' not found.")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"Unable to open or read the file: {e}")
        sys.exit(1)

    logger.info("---------- Validation Complete ----------")
    logger.info(f"Total lines processed    : {total_lines}")
    logger.info(f"Valid records            : {valid_records}")
    logger.info(f"Errors encountered       : {errors}")
    logger.info("---------------------------------------")

    if errors > 0:
        logger.error("The file contains validation errors.")
        sys.exit(1)
    elif total_lines == 0:
         logger.warning("The file is empty.")
         sys.exit(0)
    else:
        logger.info("Success: All records conform to the PoemSchema.")
        sys.exit(0)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: python {sys.argv[0]} <path_to_poems.jsonl.gz>")
        sys.exit(1)

    file_to_validate = sys.argv[1]
    validate_ndjson_file(file_to_validate)
