#!/usr/bin/env python3
"""
remove_background.py
Blanks Form XObjects that draw background branding (Resonance logo/watermark)
behind the page content. Use after remove_footer.py for footer/header removal.

Usage:
    python remove_background.py input.pdf output.pdf [--also-meta5]
"""
import argparse
import sys

try:
    import fitz
except ImportError:
    print("PyMuPDF required: pip install pymupdf")
    sys.exit(2)


GREY_BG_FILLS = (b"0.906 0.91 0.918 rg", b".906 .91 .918 rg")


def find_grey_background_xrefs(doc):
    """Find all XObject xrefs whose stream draws the Resonance grey background fill."""
    found = {}
    for pi in range(doc.page_count):
        for xobj in doc.get_page_xobjects(pi):
            xref = xobj[0]
            name = xobj[1] if len(xobj) > 1 else ""
            if xref in found:
                continue
            try:
                stream = doc.xref_stream(xref)
                if stream and any(pat in stream for pat in GREY_BG_FILLS):
                    found[xref] = name
            except Exception:
                pass
    return found


def find_xref_by_name(doc, name):
    for pi in range(min(5, doc.page_count)):
        xobjs = doc.get_page_xobjects(pi)
        for xobj in xobjs:
            xref, n = xobj[0], xobj[1] if len(xobj) > 1 else ""
            if n == name:
                return xref
    return None


def blank_xobject(doc, xref, name, verbose=False):
    try:
        doc.update_stream(xref, b"")
        if verbose:
            print(f"Blanked {name} (xref={xref})")
    except Exception as e:
        print(f"Warning: could not blank {name} (xref={xref}): {e}")


def main():
    p = argparse.ArgumentParser(
        description="Blank background XObjects (Resonance logo/watermark) from PDF."
    )
    p.add_argument("input", help="Input PDF file")
    p.add_argument("output", help="Output PDF file")
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args()

    doc = fitz.open(args.input)

    grey_xrefs = find_grey_background_xrefs(doc)
    if not grey_xrefs:
        # Fallback: try legacy name-based lookup
        xref = find_xref_by_name(doc, "Meta15")
        if xref:
            grey_xrefs = {xref: "Meta15"}

    if not grey_xrefs:
        print("No grey background XObjects found — nothing to remove.")
    else:
        if args.verbose:
            print(f"Found {len(grey_xrefs)} grey background XObject(s)")
        for xref, name in grey_xrefs.items():
            blank_xobject(doc, xref, name or f"xref={xref}", verbose=args.verbose)

    doc.save(args.output, garbage=4, deflate=True)
    doc.close()
    print(f"Saved: {args.output}")


if __name__ == "__main__":
    main()
