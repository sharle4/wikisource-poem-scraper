import json
import gzip
import sys
import logging
from pydantic import ValidationError
from src.poemscraper.schemas import PoemSchema

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def validate_ndjson_file(filepath: str):
    """
    Valide un fichier .jsonl.gz en utilisant le schéma Pydantic PoemSchema.
    """
    logger.info(f"Début de la validation pour : {filepath}")
    total_lines = 0
    valid_records = 0
    errors = 0
    
    try:
        with gzip.open(filepath, 'rt', encoding='utf-8') as f:
            for line_number, line in enumerate(f, 1):
                total_lines += 1
                line_content = line.strip()
                if not line_content:
                    logger.warning(f"Ligne {line_number}: Ligne vide, ignorée.")
                    continue

                try:
                    data = json.loads(line_content)
                    PoemSchema.model_validate(data)
                    valid_records += 1
                
                except json.JSONDecodeError as e:
                    logger.error(f"Ligne {line_number}: Erreur de décodage JSON. Détails : {e}")
                    errors += 1
                except ValidationError as e:
                    logger.error(f"Ligne {line_number}: Échec de validation du schéma.")
                    logger.error(f"  Titre : {data.get('title', 'N/A')}, PageID: {data.get('page_id', 'N/A')}")
                    for error in e.errors():
                        logger.error(f"    Champ: {'.'.join(map(str, error['loc']))}, Erreur: {error['msg']}")
                    errors += 1
                except Exception as e:
                    logger.critical(f"Ligne {line_number}: Erreur inattendue : {e}")
                    errors += 1

    except FileNotFoundError:
        logger.critical(f"Erreur : Le fichier '{filepath}' n'a pas été trouvé.")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"Impossible d'ouvrir ou de lire le fichier : {e}")
        sys.exit(1)

    logger.info("---------- Validation Terminée ----------")
    logger.info(f"Lignes totales traitées   : {total_lines}")
    logger.info(f"Enregistrements valides : {valid_records}")
    logger.info(f"Erreurs rencontrées     : {errors}")
    logger.info("---------------------------------------")

    if errors > 0:
        logger.error("Le fichier contient des erreurs de validation.")
        sys.exit(1)
    elif total_lines == 0:
         logger.warning("Le fichier est vide.")
         sys.exit(0)
    else:
        logger.info("Succès : Tous les enregistrements sont conformes au schéma PoemSchema.")
        sys.exit(0)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: python {sys.argv[0]} <chemin_vers_poems.jsonl.gz>")
        sys.exit(1)
        
    file_to_validate = sys.argv[1]
    validate_ndjson_file(file_to_validate)