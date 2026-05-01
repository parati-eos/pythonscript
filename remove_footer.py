#!/usr/bin/env python3
"""
remove_footer.py

Utility to cover or redact a footer and/or a small header logo on every page of a PDF.

Features:
- Cover the bottom area with a filled rectangle (overlay)
- Cover a small top-right header logo (overlay)
- Optional automatic footer height detection using text blocks on the first page
- Optional redaction mode which removes underlying content (use with care)

Usage examples:
    python remove_footer.py input.pdf output.pdf --height 60
    python remove_footer.py input.pdf output.pdf --percent 8
    python remove_footer.py input.pdf output.pdf --auto --verbose
    python remove_footer.py input.pdf output.pdf --auto --redact --verbose
    python remove_footer.py input.pdf output.pdf --remove-header-logo

Notes:
- Uses PyMuPDF (fitz). Install with `pip install -r requirements.txt`.
"""

import argparse
import sys
from typing import Optional, Tuple

try:
    import fitz  # PyMuPDF
except Exception:
    fitz = None


def parse_args():
    p = argparse.ArgumentParser(description="Cover or redact bottom footer area and/or a top-right header logo in a PDF.")
    p.add_argument("input", help="Input PDF file")
    p.add_argument("output", help="Output PDF file")
    grp = p.add_mutually_exclusive_group(required=False)
    grp.add_argument("--height", type=float, help="Height in points to cover from bottom")
    grp.add_argument("--percent", type=float, help="Height as percent of page height (0-100)")
    p.add_argument("--auto", action="store_true", help="Auto-detect footer height from first page text blocks")
    p.add_argument("--remove-header-logo", action="store_true",
                   help="Cover a small dark logo in the top-right header area on each page")
    p.add_argument("--logo-rect", nargs=4, type=float, metavar=("X0", "Y0", "X1", "Y1"),
                   help="Explicit header logo rectangle in PDF points. Overrides auto-detection.")
    p.add_argument("--logo-pad", type=float, default=3.0,
                   help="Padding in points around the detected or explicit header logo rectangle (default 3)")
    p.add_argument("--no-preserve-header-lines", action="store_true",
                   help="Do not redraw horizontal header lines passing through the removed logo area")
    p.add_argument("--redact", action="store_true", help="Perform true PDF redaction (removes underlying content). Use with care.")
    p.add_argument("--color", nargs=3, type=int, metavar=("R","G","B"), default=[255,255,255],
                   help="Fill color RGB 0-255 (default white)")
    p.add_argument("--verbose", action="store_true")
    return p.parse_args()


def rgb255_to_pdf(color: Tuple[int, int, int]) -> Tuple[float, float, float]:
    return tuple(c / 255.0 for c in color)


def detect_footer_height(doc, max_bottom_pct: float = 18.0, max_block_height_pct: float = 6.0, pad: float = 4.0, pages_to_scan: int = 6, verbose: bool = False) -> float:
    """Conservative footer detection across multiple pages.

    - Scan up to `pages_to_scan` first pages.
    - For each page, consider only text blocks whose bottom (y1) is within the bottom `max_bottom_pct` percent
      and whose block height (y1-y0) is <= `max_block_height_pct` percent of page height (small blocks only).
    - For each page take the minimal candidate (closest to bottom). Collect per-page heights and return the
      median across pages. This better avoids large content blocks being mistaken for footers.
    - If not enough candidates found, return 0.0 to indicate failure (caller should fallback).
    """
    if doc.page_count == 0:
        return 0.0

    scanned = min(pages_to_scan, doc.page_count)
    per_page_heights = []

    for pi in range(scanned):
        page = doc[pi]
        rect = page.rect
        page_h = rect.height
        bottom_threshold = rect.y1 - (page_h * (max_bottom_pct / 100.0))
        max_block_h = page_h * (max_block_height_pct / 100.0)

        blocks = page.get_text("blocks")
        candidates = []
        for b in blocks:
            if len(b) < 5:
                continue
            x0, y0, x1, y1 = float(b[0]), float(b[1]), float(b[2]), float(b[3])
            text = str(b[4]) if b[4] is not None else ""
            block_h = y1 - y0
            # require block to be near bottom and not too tall
            if y1 >= bottom_threshold and block_h <= max_block_h and text and text.strip():
                candidates.append((y0, y1, text.strip()))
                if verbose:
                    print(f"Page {pi+1}: footer candidate h={block_h:.2f} y0={y0:.2f} y1={y1:.2f} text='{text.strip()[:40]}'")

        if candidates:
            # choose the candidate with maximal y1 (closest to bottom) but minimal y0 among ties
            best = min(candidates, key=lambda t: t[0])
            y0 = best[0]
            page_height = rect.y1 - y0 + pad
            per_page_heights.append(page_height)

    if not per_page_heights:
        if verbose:
            print("Auto-detection: no small footer-like blocks found across scanned pages")
        return 0.0

    # pick median to be robust against outliers
    per_page_heights.sort()
    m = len(per_page_heights)
    if m % 2 == 1:
        median = per_page_heights[m // 2]
    else:
        median = 0.5 * (per_page_heights[m // 2 - 1] + per_page_heights[m // 2])

    # cap to a reasonable fraction of page height to avoid large cuts
    sample_page = doc[0]
    cap = sample_page.rect.height * 0.12
    final = min(median, cap)
    if verbose:
        print(f"Auto-detected per-page heights: {per_page_heights}")
        print(f"Auto-detected footer height (median capped): {final:.2f} pts (cap {cap:.2f})")
    return final


def cover_footer(doc: "fitz.Document", height: Optional[float], color: Tuple[int,int,int], verbose: bool = False):
    rgb = rgb255_to_pdf(color)
    for i, page in enumerate(doc, start=1):
        rect = page.rect
        page_h = rect.height
        h = height
        if h is None or h <= 0:
            if verbose:
                print(f"Skipping page {i}: non-positive height {h}")
            continue
        cover_rect = fitz.Rect(rect.x0, rect.y1 - h, rect.x1, rect.y1)
        shape = page.new_shape()
        shape.draw_rect(cover_rect)
        shape.finish(fill=rgb, color=None, width=0)
        shape.commit()
        if verbose:
            print(f"Page {i}: covered bottom {h:.2f} pts")


def redact_footer(doc: "fitz.Document", height: Optional[float], verbose: bool = False):
    # Add redaction annots for each page and apply
    for i, page in enumerate(doc, start=1):
        rect = page.rect
        h = height
        if h is None or h <= 0:
            if verbose:
                print(f"Skipping page {i}: non-positive height {h}")
            continue
        red_rect = fitz.Rect(rect.x0, rect.y1 - h, rect.x1, rect.y1)
        # add redaction annotation with white fill
        page.add_redact_annot(red_rect, fill=(1,1,1))
        if verbose:
            print(f"Page {i}: added redaction rect {red_rect}")
    # apply redactions
    for i, page in enumerate(doc, start=1):
        try:
            page.apply_redactions()
            if verbose:
                print(f"Page {i}: applied redactions")
        except Exception as e:
            if verbose:
                print(f"Page {i}: failed to apply redactions: {e}")


def padded_rect(rect: "fitz.Rect", page_rect: "fitz.Rect", pad: float) -> "fitz.Rect":
    return fitz.Rect(
        max(page_rect.x0, rect.x0 - pad),
        max(page_rect.y0, rect.y0 - pad),
        min(page_rect.x1, rect.x1 + pad),
        min(page_rect.y1, rect.y1 + pad),
    )


def is_dark_color(color) -> bool:
    return color is not None and sum(float(c) for c in color) <= 0.75


def detect_header_logo_rect(page: "fitz.Page", pad: float = 3.0, verbose: bool = False) -> Optional["fitz.Rect"]:
    """Find a small dark filled vector mark in the top-right header area."""
    page_rect = page.rect
    candidates = []
    for drawing in page.get_drawings():
        rect = drawing.get("rect")
        if rect is None or not is_dark_color(drawing.get("fill")):
            continue

        width = rect.width
        height = rect.height
        area = width * height
        in_header = rect.y0 <= page_rect.y0 + page_rect.height * 0.10
        on_right = rect.x0 >= page_rect.x0 + page_rect.width * 0.80
        plausible_size = 8 <= width <= 70 and 8 <= height <= 70 and area <= 3000
        if in_header and on_right and plausible_size:
            candidates.append((area, rect))

    if not candidates:
        return None

    _, rect = max(candidates, key=lambda item: item[0])
    result = padded_rect(rect, page_rect, pad)
    if verbose:
        print(f"Detected header logo rect: {result}")
    return result


def horizontal_lines_through_rect(page: "fitz.Page", rect: "fitz.Rect"):
    lines = []
    for drawing in page.get_drawings():
        color = drawing.get("color")
        width = drawing.get("width") or 1.0
        if not is_dark_color(color):
            continue

        for item in drawing.get("items", []):
            if not item or item[0] != "l":
                continue
            p1, p2 = item[1], item[2]
            if abs(p1.y - p2.y) > 0.2:
                continue
            y = p1.y
            x0, x1 = sorted((p1.x, p2.x))
            crosses_rect = rect.y0 <= y <= rect.y1 and x0 <= rect.x1 and x1 >= rect.x0
            if crosses_rect:
                lines.append((max(x0, rect.x0), min(x1, rect.x1), y, width, color))
    return lines


def draw_cover_rect(page: "fitz.Page", rect: "fitz.Rect", color: Tuple[float, float, float]):
    shape = page.new_shape()
    shape.draw_rect(rect)
    shape.finish(fill=color, color=None, width=0)
    shape.commit()


def redraw_lines(page: "fitz.Page", lines, verbose: bool = False):
    for x0, x1, y, width, color in lines:
        shape = page.new_shape()
        shape.draw_line(fitz.Point(x0, y), fitz.Point(x1, y))
        shape.finish(color=color, width=width)
        shape.commit()
        if verbose:
            print(f"Restored header line segment x={x0:.2f}-{x1:.2f} y={y:.2f}")


def header_logo_rect_for_page(page: "fitz.Page", explicit_rect, pad: float, verbose: bool = False) -> Optional["fitz.Rect"]:
    if explicit_rect is not None:
        return padded_rect(fitz.Rect(*explicit_rect), page.rect, pad)
    return detect_header_logo_rect(page, pad=pad, verbose=verbose)


def cover_header_logo(doc: "fitz.Document", color: Tuple[int, int, int], explicit_rect=None, pad: float = 3.0,
                      preserve_lines: bool = True, verbose: bool = False):
    rgb = rgb255_to_pdf(color)
    for i, page in enumerate(doc, start=1):
        rect = header_logo_rect_for_page(page, explicit_rect, pad=pad, verbose=verbose)
        if rect is None:
            if verbose:
                print(f"Page {i}: no header logo detected")
            continue

        lines = horizontal_lines_through_rect(page, rect) if preserve_lines else []
        draw_cover_rect(page, rect, rgb)
        redraw_lines(page, lines, verbose=verbose)
        if verbose:
            print(f"Page {i}: covered header logo rect {rect}")


def redact_header_logo(doc: "fitz.Document", explicit_rect=None, pad: float = 3.0,
                       preserve_lines: bool = True, verbose: bool = False):
    per_page_lines = []
    for i, page in enumerate(doc, start=1):
        rect = header_logo_rect_for_page(page, explicit_rect, pad=pad, verbose=verbose)
        if rect is None:
            per_page_lines.append([])
            if verbose:
                print(f"Page {i}: no header logo detected")
            continue

        per_page_lines.append(horizontal_lines_through_rect(page, rect) if preserve_lines else [])
        page.add_redact_annot(rect, fill=(1, 1, 1))
        if verbose:
            print(f"Page {i}: added header logo redaction rect {rect}")

    for i, page in enumerate(doc, start=1):
        page.apply_redactions()
        redraw_lines(page, per_page_lines[i - 1], verbose=verbose)
        if verbose:
            print(f"Page {i}: applied header logo redactions")


def main():
    args = parse_args()
    if fitz is None:
        print("PyMuPDF (fitz) is not installed. Install with: pip install -r requirements.txt")
        sys.exit(2)

    remove_footer = bool(args.height or args.percent or args.auto)
    if not remove_footer and not args.remove_header_logo and not args.logo_rect:
        print("Specify --height/--percent/--auto for footer removal, or --remove-header-logo/--logo-rect for header logo removal.")
        sys.exit(2)

    try:
        doc = fitz.open(args.input)
    except Exception as e:
        print(f"Failed to open input PDF: {e}")
        sys.exit(2)

    # determine footer height
    height_pts = None
    if args.auto:
        height_pts = detect_footer_height(doc, verbose=args.verbose)
        if height_pts <= 0:
            print("Auto-detection failed to find a footer height. Please supply --height or --percent.")
            doc.close()
            sys.exit(2)
    elif remove_footer:
        if args.percent is not None:
            # Use the first page to compute points from percent
            if doc.page_count == 0:
                print("Input PDF has no pages")
                doc.close()
                sys.exit(2)
            page0 = doc[0]
            height_pts = page0.rect.height * (args.percent / 100.0)
        elif args.height is not None:
            height_pts = args.height

    if args.verbose and height_pts is not None:
        print(f"Using height (points): {height_pts:.2f}")

    # perform overlay or redaction
    try:
        if args.redact:
            if remove_footer:
                redact_footer(doc, height_pts, verbose=args.verbose)
            if args.remove_header_logo or args.logo_rect:
                redact_header_logo(doc, explicit_rect=args.logo_rect, pad=args.logo_pad,
                                   preserve_lines=not args.no_preserve_header_lines, verbose=args.verbose)
        else:
            if remove_footer:
                cover_footer(doc, height_pts, color=tuple(args.color), verbose=args.verbose)
            if args.remove_header_logo or args.logo_rect:
                cover_header_logo(doc, color=tuple(args.color), explicit_rect=args.logo_rect, pad=args.logo_pad,
                                  preserve_lines=not args.no_preserve_header_lines, verbose=args.verbose)
        doc.save(args.output)
        doc.close()
        if args.verbose:
            print(f"Wrote output: {args.output}")
    except Exception as e:
        print(f"Error during processing: {e}")
        try:
            doc.close()
        except Exception:
            pass
        sys.exit(2)


if __name__ == '__main__':
    main()
