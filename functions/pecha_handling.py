import logging
import tempfile
import zipfile
from pathlib import Path

from openpecha.pecha import Pecha
from storage import Storage

logger = logging.getLogger(__name__)


def retrieve_pecha(pecha_id: str) -> Pecha:
    zip_path = Storage().retrieve_pecha_opf(pecha_id)

    temp_dir = tempfile.gettempdir()
    extract_path = Path(temp_dir) / "pecha_extracts"
    extract_path.mkdir(exist_ok=True)

    pecha_path = extract_path / pecha_id
    pecha_path.mkdir(exist_ok=True)

    with zipfile.ZipFile(zip_path) as zip_file:
        logger.info("Extracting ZIP to: %s", pecha_path)
        zip_file.extractall(pecha_path)

    if not (pecha_path / "base").exists():
        logger.warning("Base directory not found in zip of pecha %s, checking one level down", pecha_id)
        subdirs = [d for d in pecha_path.iterdir() if d.is_dir()]
        if subdirs and (subdirs[0] / "base").exists():
            pecha_path = subdirs[0]

    logger.info("Using pecha path: %s", pecha_path)
    return Pecha.from_path(pecha_path)


def create_tmp() -> Path:
    with tempfile.NamedTemporaryFile(delete=False) as temp:
        return Path(temp.name)
