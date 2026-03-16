"""
update.py - Detective Conan index.html sync utility.

This file patches index.html with new DoodStream and StreamP2P links and can
also do a bulk sync from provider APIs by reading file titles.
"""

import argparse
import json
import os
import re
import sys
from typing import Any

import requests

from conan_utils import xor_encrypt

DOODSTREAM_API_KEY = os.environ.get("DOODSTREAM_API_KEY", "").strip()
STREAMP2P_API_KEY = os.environ.get("STREAMP2P_API_KEY", "").strip()
HTML_FILE = os.environ.get("HTML_FILE", "index.html")
XOR_KEY = "DetectiveConan2024"

TITLE_RE = re.compile(
    r"Detective Conan\s*(Movie)?\s*[-\u2013]?\s*(\d+)\s+(HS|SS|DUB)",
    re.IGNORECASE,
)


def read_html() -> str:
    with open(HTML_FILE, "r", encoding="utf-8") as f:
        return f.read()


def write_html(content: str) -> None:
    with open(HTML_FILE, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"  Saved {HTML_FILE}")


def _replace_block_body(html: str, start_pat: str, end_pat: str, transform) -> str:
    pattern = re.compile(start_pat + r"(?P<body>.*?)" + end_pat, re.DOTALL)
    match = pattern.search(html)
    if not match:
        raise ValueError("Could not find target block in index.html")
    body = match.group("body")
    new_body = transform(body)
    return html[: match.start("body")] + new_body + html[match.end("body"):]


def patch_hs(html: str, ep: int, url: str) -> str:
    encrypted = xor_encrypt(url, XOR_KEY)
    entry_re = re.compile(rf'^(\s*){ep}:\s*".*?",?\s*$', re.MULTILINE)

    def transform(body: str) -> str:
        new_line = f'      {ep}: "{encrypted}",'
        if entry_re.search(body):
            print(f"  [EP HS] Updated episode {ep}")
            return entry_re.sub(new_line, body)
        body_with_nl = body if not body or body.endswith("\n") else body + "\n"
        print(f"  [EP HS] Inserted episode {ep}")
        return body_with_nl + new_line + "\n"

    return _replace_block_body(
        html,
        r"const ENCRYPTED_REMASTERED_HARD\s*=\s*\{\s*\n",
        r"\s*\};",
        transform,
    )


def _episode_line_re(ep: int) -> re.Pattern[str]:
    return re.compile(rf'^(?P<indent>\s*)EP_DB\[{ep}\]\s*=\s*(?P<obj>\{{.*?\}});\s*$', re.MULTILINE)


def _insert_episode_line(html: str, ep: int, obj: dict[str, Any]) -> str:
    line = f"    EP_DB[{ep}] = {json.dumps(obj, ensure_ascii=True)};"
    last_ep = None
    for match in re.finditer(r'^\s*EP_DB\[(\d+)\]\s*=\s*\{.*?\};\s*$', html, re.MULTILINE):
        last_ep = match
    if last_ep:
        insert_at = last_ep.end()
        return html[:insert_at] + "\n" + line + html[insert_at:]

    anchor = re.search(r'^\s*function\s+hasEpisodeLink\s*\(', html, re.MULTILINE)
    if anchor:
        return html[: anchor.start()] + line + "\n" + html[anchor.start():]

    return html + "\n" + line + "\n"


def _update_episode_field(html: str, ep: int, mode: str, field: str, url: str) -> str:
    line_re = _episode_line_re(ep)
    match = line_re.search(html)

    if not match:
        obj = {"original": {}, "remastered": {}}
        obj.setdefault(mode, {})
        obj[mode][field] = url
        print(f"  [EP {mode.upper()} {field.upper()}] Inserted episode {ep}")
        return _insert_episode_line(html, ep, obj)

    obj = json.loads(match.group("obj"))
    obj.setdefault("original", {})
    obj.setdefault("remastered", {})
    obj.setdefault(mode, {})
    obj[mode][field] = url

    indent = match.group("indent")
    new_line = f"{indent}EP_DB[{ep}] = {json.dumps(obj, ensure_ascii=True)};"
    print(f"  [EP {mode.upper()} {field.upper()}] Updated episode {ep}")
    return html[: match.start()] + new_line + html[match.end():]


def patch_ss(html: str, ep: int, url: str) -> str:
    return _update_episode_field(html, ep, "original", "soft", url)


def patch_hs_s2(html: str, ep: int, url: str) -> str:
    return _update_episode_field(html, ep, "remastered", "hard2", url)


def patch_ss_s2(html: str, ep: int, url: str) -> str:
    return _update_episode_field(html, ep, "original", "soft2", url)


def _movie_pattern(num: int, mode: str, field: str) -> re.Pattern[str]:
    return re.compile(
        rf'^([ \t]*MOVIE_DB\[{num}\]\.{mode}\.{field}\s*=\s*)"[^"]*"(;.*)?\s*$',
        re.MULTILINE,
    )


def _movie_anchor(html: str) -> int:
    anchor = re.search(r'^\s*const\s+EP_DB\s*=\s*\{\};\s*$', html, re.MULTILINE)
    if anchor:
        return anchor.start()
    match = None
    for match in re.finditer(r'^\s*MOVIE_DB\[\d+\]\.(?:original|remastered)\.(?:hard|soft|dub|hard2|soft2|dub2)\s*=.*$', html, re.MULTILINE):
        pass
    if match:
        return match.end() + 1
    return len(html)


def _patch_movie_field(html: str, num: int, mode: str, field: str, url: str, tag: str) -> str:
    pat = _movie_pattern(num, mode, field)
    new_line = f'    MOVIE_DB[{num}].{mode}.{field} = "{url}"; // Movie {num} {tag}'
    if pat.search(html):
        print(f"  [MV {tag}] Updated movie {num}")
        return pat.sub(new_line, html)
    print(f"  [MV {tag}] Inserted movie {num}")
    anchor = _movie_anchor(html)
    return html[:anchor] + new_line + "\n" + html[anchor:]


def patch_movie_hs(html: str, num: int, url: str) -> str:
    return _patch_movie_field(html, num, "original", "hard", url, "HS")


def patch_movie_ss(html: str, num: int, url: str) -> str:
    return _patch_movie_field(html, num, "original", "soft", url, "SS")


def patch_movie_hs_s2(html: str, num: int, url: str) -> str:
    return _patch_movie_field(html, num, "original", "hard2", url, "HS S2")


def patch_movie_ss_s2(html: str, num: int, url: str) -> str:
    return _patch_movie_field(html, num, "original", "soft2", url, "SS S2")


def apply_patch(
    ep: int | None = None,
    movie: int | None = None,
    hs_url: str | None = None,
    ss_url: str | None = None,
    hs_url_s2: str | None = None,
    ss_url_s2: str | None = None,
) -> None:
    if not hs_url and not ss_url and not hs_url_s2 and not ss_url_s2:
        print("Nothing to patch.")
        return

    html = read_html()
    if ep is not None:
        if hs_url:
            html = patch_hs(html, ep, hs_url)
        if ss_url:
            html = patch_ss(html, ep, ss_url)
        if hs_url_s2:
            html = patch_hs_s2(html, ep, hs_url_s2)
        if ss_url_s2:
            html = patch_ss_s2(html, ep, ss_url_s2)
    elif movie is not None:
        if hs_url:
            html = patch_movie_hs(html, movie, hs_url)
        if ss_url:
            html = patch_movie_ss(html, movie, ss_url)
        if hs_url_s2:
            html = patch_movie_hs_s2(html, movie, hs_url_s2)
        if ss_url_s2:
            html = patch_movie_ss_s2(html, movie, ss_url_s2)

    write_html(html)


def _extract_url(value: Any) -> str:
    if isinstance(value, str):
        candidate = value.strip()
        if candidate.startswith("http://") or candidate.startswith("https://"):
            return candidate
        return ""
    if isinstance(value, dict):
        for key in (
            "embed_url", "embed", "player_url", "protected_embed", "download_url",
            "url", "video_url", "iframe", "src", "link",
        ):
            if key in value:
                found = _extract_url(value.get(key))
                if found:
                    return found
        for nested in value.values():
            found = _extract_url(nested)
            if found:
                return found
    if isinstance(value, list):
        for item in value:
            found = _extract_url(item)
            if found:
                return found
    return ""


def _extract_title(value: Any) -> str:
    if isinstance(value, dict):
        for key in ("title", "name", "filename", "file_name", "original_filename"):
            if key in value and isinstance(value[key], str):
                return value[key].strip()
    return ""


def _extract_rows(value: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if isinstance(value, dict):
        if any(key in value for key in ("title", "name", "filename", "file_name", "original_filename")):
            rows.append(value)
        for nested in value.values():
            rows.extend(_extract_rows(nested))
    elif isinstance(value, list):
        for item in value:
            rows.extend(_extract_rows(item))
    return rows


def fetch_all_dood_files() -> list[dict[str, Any]]:
    if not DOODSTREAM_API_KEY:
        return []

    files: list[dict[str, Any]] = []
    page = 1
    while True:
        try:
            resp = requests.get(
                "https://doodapi.co/api/file/list",
                params={"key": DOODSTREAM_API_KEY, "page": page, "per_page": 200},
                timeout=30,
            ).json()
        except Exception as exc:
            print(f"  DoodStream API error (page {page}): {exc}", file=sys.stderr)
            break

        if resp.get("status") != 200:
            break

        result = resp.get("result") or {}
        rows = result.get("results") or []
        if not rows:
            break

        files.extend(rows)
        if page >= int(result.get("pages", 1)):
            break
        page += 1

    return files


def _streamp2p_headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {STREAMP2P_API_KEY}",
        "Accept": "application/json",
    }


def fetch_all_streamp2p_files() -> list[dict[str, Any]]:
    if not STREAMP2P_API_KEY:
        return []

    base = "https://streamp2p.com"
    endpoints = [
        "/api/v1/video",
        "/api/v1/videos",
        "/api/v1/video/list",
        "/api/v1/videos/list",
    ]
    params_variants = [
        {"page": 1, "per_page": 200},
        {"page": 1, "limit": 200},
        {},
    ]

    collected: list[dict[str, Any]] = []
    seen_rows: set[tuple[str, str]] = set()

    for endpoint in endpoints:
        for params in params_variants:
            try:
                resp = requests.get(f"{base}{endpoint}", headers=_streamp2p_headers(), params=params, timeout=30)
            except Exception as exc:
                print(f"  StreamP2P list error {endpoint}: {exc}", file=sys.stderr)
                continue

            if resp.status_code >= 400:
                continue

            try:
                data = resp.json()
            except Exception:
                continue

            rows = _extract_rows(data)
            if not rows:
                continue

            for row in rows:
                title = _extract_title(row)
                url = _extract_url(row)
                key = (title, url)
                if key in seen_rows:
                    continue
                seen_rows.add(key)
                collected.append(row)

            if collected:
                print(f"  StreamP2P sync source: {endpoint} ({len(collected)} row candidates)")
                return collected

    print("  StreamP2P bulk sync list endpoint not detected", file=sys.stderr)
    return collected


def _sync_sort_key(item: tuple[str, bool, int, str, str, str]) -> tuple[int, int, int, int, str]:
    provider, is_movie, num, kind, title, url = item
    kind_order = {"HS": 0, "SS": 1, "DUB": 2}.get(kind, 9)
    provider_order = 0 if provider == "dood" else 1
    return (1 if is_movie else 0, int(num), provider_order, kind_order, title.lower())


def bulk_sync() -> int:
    parsed: list[tuple[str, bool, int, str, str, str]] = []

    if DOODSTREAM_API_KEY:
        print("Fetching all DoodStream files...")
        dood_files = fetch_all_dood_files()
        print(f"  Found {len(dood_files)} total DoodStream files")
        for row in dood_files:
            title = (row.get("title") or "").strip()
            match = TITLE_RE.search(title)
            if not match:
                continue
            is_movie = bool(match.group(1))
            num = int(match.group(2))
            kind = match.group(3).upper()
            url = row.get("download_url") or row.get("embed_url") or row.get("protected_embed") or ""
            if url:
                parsed.append(("dood", is_movie, num, kind, title, url))

    if STREAMP2P_API_KEY:
        print("Fetching all StreamP2P files...")
        streamp2p_files = fetch_all_streamp2p_files()
        print(f"  Found {len(streamp2p_files)} total StreamP2P file candidates")
        for row in streamp2p_files:
            title = _extract_title(row)
            match = TITLE_RE.search(title)
            if not match:
                continue
            is_movie = bool(match.group(1))
            num = int(match.group(2))
            kind = match.group(3).upper()
            url = _extract_url(row)
            if url:
                parsed.append(("s2", is_movie, num, kind, title, url))

    parsed.sort(key=_sync_sort_key)

    html = read_html()
    patched = 0
    for provider, is_movie, num, kind, title, url in parsed:
        if is_movie:
            if provider == "dood":
                if kind == "HS":
                    html = patch_movie_hs(html, num, url)
                elif kind in ("SS", "DUB"):
                    html = patch_movie_ss(html, num, url)
            else:
                if kind == "HS":
                    html = patch_movie_hs_s2(html, num, url)
                elif kind in ("SS", "DUB"):
                    html = patch_movie_ss_s2(html, num, url)
        else:
            if provider == "dood":
                if kind == "HS":
                    html = patch_hs(html, num, url)
                elif kind in ("SS", "DUB"):
                    html = patch_ss(html, num, url)
            else:
                if kind == "HS":
                    html = patch_hs_s2(html, num, url)
                elif kind in ("SS", "DUB"):
                    html = patch_ss_s2(html, num, url)
        patched += 1

    if patched:
        write_html(html)
        print(f"  Bulk sync complete - {patched} entries updated")
    else:
        print("  No matching files found")

    return patched


def main() -> None:
    parser = argparse.ArgumentParser(description="Patch index.html with provider links")
    parser.add_argument("--ep", type=int, help="Episode number")
    parser.add_argument("--movie", type=int, help="Movie number")
    parser.add_argument("--hs", metavar="URL", help="DoodStream hard-sub URL")
    parser.add_argument("--ss", metavar="URL", help="DoodStream soft-sub URL")
    parser.add_argument("--hs-s2", metavar="URL", help="StreamP2P hard-sub URL")
    parser.add_argument("--ss-s2", metavar="URL", help="StreamP2P soft-sub URL")
    parser.add_argument("--bulk-sync", action="store_true", help="Sync all files from provider APIs")
    args = parser.parse_args()

    if args.bulk_sync:
        bulk_sync()
    elif args.ep is not None or args.movie is not None:
        apply_patch(
            ep=args.ep,
            movie=args.movie,
            hs_url=args.hs,
            ss_url=args.ss,
            hs_url_s2=args.hs_s2,
            ss_url_s2=args.ss_s2,
        )
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
