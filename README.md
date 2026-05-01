remove_footer.py

Covers/removes a footer on each page of a PDF by drawing a filled rectangle over the bottom of each page. It can also cover a small top-right header logo.

Usage

    python remove_footer.py input.pdf output.pdf --height 60
    python remove_footer.py input.pdf output.pdf --percent 8 --color 255 255 255
    python remove_footer.py input.pdf output.pdf --remove-header-logo
    python remove_footer.py input.pdf output.pdf --percent 8 --remove-header-logo

Install dependencies

    pip install -r requirements.txt

Notes

- This overlays a rectangle on top of the page content; it does not redact or remove underlying objects from the PDF file. For legal-grade redaction use dedicated PDF redaction tools.
- Choose `--height` in points (1 point = 1/72 inch) or `--percent` of page height.
