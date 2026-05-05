"""
app.py — PDF footer & header-logo removal API + UI

Run:
    uvicorn app:app --reload --port 8000
"""

import json
import tempfile
import uuid
from pathlib import Path
from typing import Annotated, List, Optional

try:
    import fitz
except ImportError:
    fitz = None

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse

from remove_footer import cover_footer, cover_header_logo, detect_footer_height
from smartleads_clay import router as smartleads_router

app = FastAPI(title="PDF Cleaner")
app.include_router(smartleads_router)

_OUTPUT_DIR = Path(tempfile.mkdtemp(prefix="pdf_cleaner_"))


def _process_pdf(src: Path, dst: Path, footer_height: Optional[int] = None) -> None:
    doc = fitz.open(str(src))
    height_pts = footer_height if footer_height is not None else detect_footer_height(doc)
    if height_pts > 0:
        cover_footer(doc, height_pts, color=(255, 255, 255))
    cover_header_logo(doc, color=(255, 255, 255))
    doc.save(str(dst))
    doc.close()


@app.get("/health")
async def health():
    return {"status": "ok", "pymupdf": fitz is not None}


@app.get("/", response_class=HTMLResponse)
async def index():
    html_path = Path(__file__).parent / "templates" / "index.html"
    return HTMLResponse(html_path.read_text())


@app.post("/detect-height")
async def detect_height(file: Annotated[UploadFile, File()]):
    if fitz is None:
        raise HTTPException(500, "PyMuPDF is not installed")
    raw = await file.read()
    tmp = _OUTPUT_DIR / f"{uuid.uuid4().hex}_probe.pdf"
    try:
        tmp.write_bytes(raw)
        doc = fitz.open(str(tmp))
        h = detect_footer_height(doc)
        doc.close()
        return {"footer_height_pts": round(h, 1)}
    finally:
        tmp.unlink(missing_ok=True)


@app.post("/process")
async def process_files(
    files: Annotated[List[UploadFile], File()],
    footer_height: Annotated[Optional[int], Form()] = None,
):
    if fitz is None:
        raise HTTPException(500, "PyMuPDF is not installed")

    async def _stream():
        for upload in files:
            name = upload.filename or "file.pdf"
            file_id = uuid.uuid4().hex

            yield json.dumps({"name": name, "status": "processing"}) + "\n"

            try:
                raw = await upload.read()
                src = _OUTPUT_DIR / f"{file_id}_in.pdf"
                dst = _OUTPUT_DIR / f"{file_id}_out.pdf"
                src.write_bytes(raw)
                _process_pdf(src, dst, footer_height=footer_height)
                src.unlink(missing_ok=True)

                yield json.dumps({
                    "name": name,
                    "status": "done",
                    "file_id": file_id,
                    "download_url": f"/download/{file_id}",
                }) + "\n"
            except Exception as exc:
                yield json.dumps({"name": name, "status": "error", "error": str(exc)}) + "\n"

    return StreamingResponse(_stream(), media_type="application/x-ndjson")


@app.get("/download/{file_id}")
async def download(file_id: str):
    # Reject any path traversal attempt
    if not file_id.isalnum():
        raise HTTPException(400, "Invalid file id")
    path = _OUTPUT_DIR / f"{file_id}_out.pdf"
    if not path.exists():
        raise HTTPException(404, "File not found")
    return FileResponse(
        str(path),
        media_type="application/pdf",
        filename=f"cleaned_{file_id[:8]}.pdf",
    )
