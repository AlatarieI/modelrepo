# importer.py
"""
Importer:
- scans INPUT_DIR for .zip files (or subdirectories),
- unpacks each zip into a temp folder,
- finds required files (txt metadata, geometry, preview image),
- normalizes model name, creates final_models/<model_name>/,
- moves files into final folder (with safe filenames),
- inserts DB record via db_manager.insert_model inside a safe transaction,
- on failure: rollback DB and delete partial folder, log error and continue.
"""

import os
import sys
import shutil
import zipfile
import tempfile
import re
import logging
from datetime import datetime
from pathlib import Path

from database_manager import init_database, insert_model

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


# Allowed preview extension
PREVIEW_EXTS = {".jpg", ".jpeg"}

# Allowed geometry extensions
GEOM_EXTS = {".obj", ".fbx", ".gltf", ".3ds"}


def try_open(path):
    """
    Try opening file; return text content.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        return content
    except Exception:
        pass
    raise UnicodeDecodeError("Failed to open file", path)


def parse_metadata_from_text(text):
    """
    Parse the metadata from the info text.
    """
    data = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or not line.startswith("#"):
            continue
        # Split only on first colon
        if ":" not in line:
            continue
        key, val = line[1:].split(":", 1)
        key = key.strip()
        val = val.strip()
        data[key] = val
    return data


def parse_date(date_str):
    """
    Accept multiple date formats:
      - dd.mm.yyyy or d.m.yyyy
      - dd/mm/yyyy
      - dd-mm-yyyy
      - month names not supported (keeps simple)
    Returns datetime.date or None.
    """
    if not date_str:
        return None
    date_str = date_str.strip()
    formats = ["%d.%m.%Y", "%d/%m/%Y", "%d-%m-%Y"]
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt).date()
            return dt
        except Exception:
            continue
    return None


def slugify(name):
    """
    Create a safe directory/name from arbitrary string.
    Replace spaces with underscores, remove unsafe characters, limit length.
    """
    if not name:
        return None
    s = name.strip()
    # remove file extension if present
    s = re.sub(r"\.[a-zA-Z0-9]{1,5}$", "", s)
    s = s.replace(" ", "_")
    # keep letters numbers underscore and dash
    s = re.sub(r"[^A-Za-z0-9_\-]", "", s)
    if not s:
        s = "model"
    return s[:150]


def find_preview_file(files):
    """Return filename of a preview if exists (jpg/png). Prefer jpg/jpeg first, else png."""
    # files are Path objects or strings
    # return the first matching extension in prioritized order
    lower = [(f, Path(f).suffix.lower()) for f in files]
    # prioritize jpg/jpeg
    for f, ext in lower:
        if ext in (".jpg", ".jpeg"):
            return f
    for f, ext in lower:
        if ext == ".png":
            return f
    return None


def find_geometry_file(files):
    for f in files:
        ext = Path(f).suffix.lower()
        if ext in GEOM_EXTS:
            return f
    return None


def ensure_unique_dir(base_out_dir, desired_name):
    """
    If base_out_dir/desired_name exists, append suffix _1, _2 etc.
    """
    candidate = Path(base_out_dir) / desired_name
    i = 1
    while candidate.exists():
        candidate = Path(base_out_dir) / f"{desired_name}_{i}"
        i += 1
    return candidate


def process_one_zip(zip_path: Path, final_base_dir: Path, tmp_base: Path):
    """
    Process single zip file. Raises on error.
    Returns (model_name, model_dir_path, db_model_data)
    """
    logging.info(f"Processing {zip_path}")
    # 1) extract to temp dir
    with tempfile.TemporaryDirectory(dir=tmp_base) as tmpdir:
        tmpdir_path = Path(tmpdir)
        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                zf.extractall(path=tmpdir)
        except zipfile.BadZipFile:
            raise RuntimeError(f"Bad zip file: {zip_path}")

        # collect all files extracted (flatten from subfolders)
        extracted_files = [p for p in tmpdir_path.rglob('*') if p.is_file()]
        if not extracted_files:
            raise RuntimeError(f"No files found in zip {zip_path}")

        # find metadata .txt file (first .txt)
        txt_files = [p for p in extracted_files if p.suffix.lower() == ".txt"]
        if not txt_files:
            raise RuntimeError(f"No metadata .txt found in {zip_path}")
        # prefer file with same base name as zip if present
        metadata_file = None
        base_name = zip_path.stem.lower()
        for p in txt_files:
            if p.stem.lower() == base_name:
                metadata_file = p
                break
        if metadata_file is None:
            metadata_file = txt_files[0]

        # read metadata trying encodings
        text= try_open(metadata_file)
        logging.debug("Read metadata")

        parsed = parse_metadata_from_text(text)

        # required fields we expect to be present in .txt
        geom_file_name = parsed.get("GeometryFile")
        preview_hint = parsed.get("PreviewFile")
        download_format = parsed.get("DownloadModelFormat")
        num_polys = parsed.get("NumberOfPolygons")
        download_url = parsed.get("DownloadedFromURL")
        date_of_download = parsed.get("DateOfDownload")
        created_by = parsed.get("CreatedBy")
        created_in = parsed.get("CreatedIn")
        uploaded_by = parsed.get("UploadedBy")
        description = parsed.get("Description")

        # find geometry file on extracted_files. GeometryFile may be present or not.
        geometry_path = None
        if geom_file_name:
            for p in extracted_files:
                if p.name == geom_file_name:
                    geometry_path = p
                    break
        if not geometry_path:
            # fallback: find first geometry by extension
            g = find_geometry_file([str(p) for p in extracted_files])
            if g:
                geometry_path = Path(g)

        if not geometry_path:
            raise RuntimeError(f"No geometry file found for {zip_path} (expected {geom_file_name})")

        # find preview file:
        preview_path = None
        if preview_hint:
            for p in extracted_files:
                if p.name == preview_hint:
                    preview_path = p
                    break
        if preview_path is None:
            # fallback: any jpg/png
            p = find_preview_file([str(p) for p in extracted_files])
            if p:
                preview_path = Path(p)

        if not preview_path:
            raise RuntimeError(f"No preview image found (jpg/png) in {zip_path}")

        # determine model name: prefer geometry base name, else zip stem, else metadata name
        if geom_file_name:
            model_base = Path(geom_file_name).stem
        else:
            model_base = zip_path.stem
        # fallback if Description field used as name (rare)
        if not model_base or model_base.strip() == "":
            model_base = description or zip_path.stem

        model_name_safe = slugify(model_base)
        # ensure unique directory in final_base_dir
        target_dir = ensure_unique_dir(final_base_dir, model_name_safe)
        logging.info(f"Planned final directory: {target_dir}")

        # create target directory
        target_dir.mkdir(parents=True, exist_ok=False)  # will error if race condition

        # move/copy required files into target_dir, and also move all extracted files
        try:
            # normalize filenames and move
            moved_preview_name = None
            for src in extracted_files:
                # decide destination filename (preserve extension)
                dest_name = slugify(src.stem) + src.suffix.lower()
                dest_path = target_dir / dest_name

                # if dest already exists, append _1 style
                k = 1
                while dest_path.exists():
                    dest_path = target_dir / f"{dest_name.rsplit('.',1)[0]}_{k}{src.suffix.lower()}"
                    k += 1

                shutil.move(str(src), str(dest_path))

                # track preview file name as stored (relative filename)
                if src.resolve() == preview_path.resolve() or src.name == preview_path.name:
                    moved_preview_name = dest_path.name

            # final sanity check
            if not moved_preview_name:
                raise RuntimeError("Failed to detect preview file after moving files.")

            # parse and prepare model_data for DB insertion
            model_data = {
                "model_name": target_dir.name,
                "format": (download_format or Path(geometry_path).suffix.lstrip(".")).upper(),
                "source_url": download_url,
                "download_date": parse_date(date_of_download) if date_of_download else None,
                "created_by": created_by,
                "created_in": created_in,
                "uploaded_by": uploaded_by,
                "model_description": description,
                "polygon_count": int(num_polys) if num_polys and num_polys.isdigit() else (int(float(num_polys)) if num_polys else None),
                # store preview filename (just the name inside model folder)
                "preview_file": moved_preview_name
            }

            return target_dir, model_data

        except Exception as e:
            # On failure while moving files: try to remove created dir if it's present
            try:
                if target_dir.exists():
                    shutil.rmtree(target_dir)
            except Exception:
                pass
            raise e


def process_all_zips(input_dir: Path, final_base_dir: Path, tmp_base: Path):
    """
    Iterate .zip files in input_dir (non-recursive) and process them.
    """
    init_database()  # ensure DB schema present

    input_dir = Path(input_dir)
    final_base_dir = Path(final_base_dir)
    final_base_dir.mkdir(parents=True, exist_ok=True)
    tmp_base = Path(tmp_base)
    tmp_base.mkdir(parents=True, exist_ok=True)

    zip_paths = []
    for root, dirs, files in os.walk(input_dir):
        for f in files:
            if f.lower().endswith(".zip"):
                zip_paths.append(Path(root) / f)

    logging.info(f"Found {len(zip_paths)} zip files to process in {input_dir}")

    for zp in zip_paths:
        try:
            target_dir, model_data = process_one_zip(zp, final_base_dir, tmp_base)
            # Insert DB record inside try/except so we can rollback and remove folder if DB insert fails.
            try:
                model_id = insert_model(model_data)
                logging.info(f"Inserted model id {model_id} for {model_data['model_name']}")
            except Exception as db_exc:
                # Remove created folder if DB insertion failed
                logging.error(f"Database insert failed for {model_data.get('model_name')}: {db_exc}")
                try:
                    if target_dir.exists():
                        shutil.rmtree(target_dir)
                        logging.info(f"Removed partial folder {target_dir} due to DB error.")
                except Exception as rr:
                    logging.error(f"Failed to remove partial folder {target_dir}: {rr}")
                continue

            # Optionally move processed zip to processed folder to avoid reprocessing
            processed_dir = input_dir / "_processed"
            processed_dir.mkdir(exist_ok=True)
            dst = processed_dir / zp.name
            shutil.move(str(zp), str(dst))
            logging.info(f"Moved processed zip {zp.name} -> {dst}")
        except Exception as exc:
            logging.error(f"Failed to process {zp.name}: {exc}")
            # move bad zip to failed folder for inspection
            failed_dir = input_dir / "_failed"
            failed_dir.mkdir(exist_ok=True)
            try:
                dst = failed_dir / zp.name
                shutil.move(str(zp), str(dst))
                logging.info(f"Moved failed zip {zp.name} -> {dst}")
            except Exception as mv_exc:
                logging.error(f"Failed to move failed zip {zp.name}: {mv_exc}")


if __name__ == "__main__":
    input_dir = Path("../data/download")
    output_dir = Path("../data/models")
    tmp_dir = Path("model_import_tmp") 

    process_all_zips(input_dir, output_dir, tmp_dir)
