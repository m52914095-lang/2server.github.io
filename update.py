"""
update.py - Detective Conan index.html sync utility.

This file patches index.html with DoodStream links and StreamP2P links,
and can also do a bulk sync by reading provider APIs.
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
STREAMP2P_PLAYER_URL = os.environ.get("STREAMP2P_PLAYER_URL", "").strip()
HTML_FILE = os.environ.get("HTML_FILE", "index.html")
XOR_KEY = "DetectiveConan2024"
STREAM_API_BASE = "https://streamp2p.com/api/v1"

TITLE_RE = re.compile(
    r"Detective Conan\s*(Movie)?\s*[-\u2013]?\s*(\d+)\s+(HS|SS|DUB)",
    re.IGNORECASE,
)


def read_html() -> str:
    with open(HTML_FILE, "r", encoding="utf-8") as handle:
        return handle.read()


def write_html(content: str) -> None:
    with open(HTML_FILE, "w", encoding="utf-8") as handle:
        handle.write(content)
    print(f"  Saved {HTML_FILE}")


def _replace_block_body(html: str, start_pat: str, end_pat: str, transform) -> str:
    pattern = re.compile(start_pat + r"(?P<body>.*?)" + end_pat, re.DOTALL)
    match = pattern.search(html)
    if not match:
        raise ValueError("Could not find target block in index.html")
    body = match.group("body")
    new_body = transform(body)
    return html[: match.start("body")] + new_body + html[match.end("body"):]


def _episode_line_re(ep: int) -> re.Pattern:
    return re.compile(rf"^(?P<indent>\s*)EP_DB\[{ep}\]\s*=\s*(?P<obj>\{{.*?\}});\s*$", re.MULTILINE)


def _insert_episode_line(html: str, ep: int, obj: dict[str, Any]) -> str:
    line = f"    EP_DB[{ep}] = {json.dumps(obj, ensure_ascii=True)};"
    last_ep = None
    for match in re.finditer(r"^\s*EP_DB\[(\d+)\]\s*=\s*\{.*?\};\s*$", html, re.MULTILINE):
        last_ep = match
    if last_ep:
        insert_at = last_ep.end()
        return html[:insert_at] + "\n" + line + html[insert_at:]

    anchor = re.search(r"^\s*function\s+hasEpisodeLink\s*\(", html, re.MULTILINE)
    if anchor:
        return html[: anchor.start()] + line + "\n" + html[anchor.start():]

    return html + "\n" + line + "\n"


def _update_episode_field(html: str, ep: int, mode: str, field: str, url: str) -> str:
    line_re = _episode_line_re(ep)
    match = line_re.search(html)

    if not match:
        obj = {"original": {}, "remastered": {}}
        obj.setdefault(mode, {})[field] = url
        print(f"  [EP {field.upper()}] Inserted episode {ep}")
        return _insert_episode_line(html, ep, obj)

    obj = json.loads(match.group("obj"))
    obj.setdefault("original", {})
    obj.setdefault("remastered", {})
    obj.setdefault(mode, {})
    obj[mode][field] = url

    indent = match.group("indent")
    new_line = f"{indent}EP_DB[{ep}] = {json.dumps(obj, ensure_ascii=True)};"
    print(f"  [EP {field.upper()}] Updated episode {ep}")
    return html[: match.start()] + new_line + html[match.end():]


def _patch_encrypted_hs(html: str, ep: int, url: str) -> str:
    encrypted = xor_encrypt(url, XOR_KEY)
    entry_re = re.compile(rf"^(\s*){ep}:\s*\".*?\",?\s*$", re.MULTILINE)

    def transform(body: str) -> str:
        new_line = f"      {ep}: \"{encrypted}\","
        if entry_re.search(body):
            return entry_re.sub(new_line, body)
        if body and not body.endswith("\n"):
            body = body + "\n"
        return body + new_line + "\n"

    return _replace_block_body(
        html,
        r"const ENCRYPTED_REMASTERED_HARD\s*=\s*\{\s*\n",
        r"\s*\};",
        transform,
    )


def patch_hs(html: str, ep: int, url: str) -> str:
    html = _update_episode_field(html, ep, "original", "hard", url)
    return _patch_encrypted_hs(html, ep, url)


def patch_ss(html: str, ep: int, url: str) -> str:
    return _update_episode_field(html, ep, "original", "soft", url)


def patch_hs2(html: str, ep: int, url: str) -> str:
    return _update_episode_field(html, ep, "original", "hard2", url)


def patch_ss2(html: str, ep: int, url: str) -> str:
    return _update_episode_field(html, ep, "original", "soft2", url)


def _movie_pattern(num: int, field: str) -> re.Pattern:
    return re.compile(
        rf'^([ \t]*MOVIE_DB\[{num}\]\.original\.{field}\s*=\s*)"[^"]*"(;.*)?\s*$',
        re.MULTILINE,
    )


def _movie_anchor(html: str) -> int:
    anchor = re.search(r"^\s*const\s+EP_DB\s*=\s*\{\};\s*$", html, re.MULTILINE)
    if anchor:
        return anchor.start()
    match = None
    for match in re.finditer(r"^\s*MOVIE_DB\[\d+\]\.original\.(?:hard|soft|hard2|soft2)\s*=.*$", html, re.MULTILINE):
        pass
    if match:
        return match.end() + 1
    return len(html)


def _patch_movie_line(html: str, num: int, field: str, url: str, label: str) -> str:
    pat = _movie_pattern(num, field)
    new_line = f'    MOVIE_DB[{num}].original.{field} = "{url}"; // Movie {num} {label}'
    if pat.search(html):
        print(f"  [MV {label}] Updated movie {num}")
        return pat.sub(new_line, html)
    print(f"  [MV {label}] Inserted movie {num}")
    anchor = _movie_anchor(html)
    return html[:anchor] + new_line + "\n" + html[anchor:]


def patch_movie_hs(html: str, num: int, url: str) -> str:
    return _patch_movie_line(html, num, "hard", url, "HS")


def patch_movie_ss(html: str, num: int, url: str) -> str:
    return _patch_movie_line(html, num, "soft", url, "SS")


def patch_movie_hs2(html: str, num: int, url: str) -> str:
    return _patch_movie_line(html, num, "hard2", url, "HS S2")


def patch_movie_ss2(html: str, num: int, url: str) -> str:
    return _patch_movie_line(html, num, "soft2", url, "SS S2")


def apply_patch(
    ep: int | None = None,
    movie: int | None = None,
    hs_url: str | None = None,
    ss_url: str | None = None,
    hs2_url: str | None = None,
    ss2_url: str | None = None,
) -> None:
    if not hs_url and not ss_url and not hs2_url and not ss2_url:
        print("Nothing to patch.")
        return

    html = read_html()

    if ep is not None:
        if hs_url:
            html = patch_hs(html, ep, hs_url)
        if ss_url:
            html = patch_ss(html, ep, ss_url)
        if hs2_url:
            html = patch_hs2(html, ep, hs2_url)
        if ss2_url:
            html = patch_ss2(html, ep, ss2_url)
    elif movie is not None:
        if hs_url:
            html = patch_movie_hs(html, movie, hs_url)
        if ss_url:
            html = patch_movie_ss(html, movie, ss_url)
        if hs2_url:
            html = patch_movie_hs2(html, movie, hs2_url)
        if ss2_url:
            html = patch_movie_ss2(html, movie, ss2_url)

    write_html(html)


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


def _stream_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {STREAMP2P_API_KEY}", "Accept": "application/json"}


def _stream_get(path: str, params: dict[str, Any] | None = None) -> Any | None:
    if not STREAMP2P_API_KEY:
        return None
    try:
        response = requests.get(f"{STREAM_API_BASE}{path}", headers=_stream_headers(), params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as exc:
        print(f"  StreamP2P API error ({path}): {exc}", file=sys.stderr)
        return None


def _walk_json(value: Any):
    if isinstance(value, dict):
        yield value
        for item in value.values():
            yield from _walk_json(item)
    elif isinstance(value, list):
        for item in value:
            yield from _walk_json(item)


def _collect_urls(value: Any) -> list[str]:
    urls: list[str] = []
    for item in _walk_json(value):
        if isinstance(item, dict):
            for candidate in item.values():
                if isinstance(candidate, str) and candidate.startswith(("http://", "https://")):
                    urls.append(candidate)
        elif isinstance(item, str) and item.startswith(("http://", "https://")):
            urls.append(item)
    deduped: list[str] = []
    seen: set[str] = set()
    for url in urls:
        if url not in seen:
            seen.add(url)
            deduped.append(url)
    return deduped


def _normalize_player_base(value: str | None) -> str | None:
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    if not raw.startswith(("http://", "https://")):
        raw = "https://" + raw.lstrip("/")
    parsed = requests.utils.urlparse(raw)
    if not parsed.netloc:
        return None
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path.rstrip('/')}"


def _get_stream_player_base() -> str | None:
    if STREAMP2P_PLAYER_URL:
        normalized = _normalize_player_base(STREAMP2P_PLAYER_URL)
        if normalized:
            return normalized
    payload = _stream_get("/video/player/default")
    if payload is None:
        return None
    for url in _collect_urls(payload):
        normalized = _normalize_player_base(url)
        if normalized:
            return normalized
    for item in _walk_json(payload):
        if not isinstance(item, dict):
            continue
        for key in ("domain", "hostname", "host", "subdomain"):
            candidate = item.get(key)
            if isinstance(candidate, str) and "." in candidate and " " not in candidate:
                normalized = _normalize_player_base(candidate)
                if normalized:
                    return normalized
    return None


def _build_stream_url(video_id: str) -> str | None:
    base = _get_stream_player_base()
    if not base or not video_id:
        return None
    if "#" in base:
        return base.split("#", 1)[0] + f"#{video_id}"
    return base.rstrip("/") + f"/#{video_id}".replace("/#", "#")


def _extract_stream_video_id(value: Any) -> str | None:
    for item in _walk_json(value):
        if not isinstance(item, dict):
            continue
        for key in ("videoId", "video_id", "id"):
            candidate = item.get(key)
            if candidate is not None:
                return str(candidate)
        videos = item.get("videos")
        if isinstance(videos, list) and videos:
            return str(videos[0])
    return None


def _extract_stream_url(value: Any) -> str | None:
    for url in _collect_urls(value):
        lower = url.lower()
        if any(token in lower for token in ("playerp2p.com", "streamp2p.com/e/", "embed", "/play", "#")):
            return url
    return None


def fetch_all_streamp2p_files() -> list[dict[str, Any]]:
    if not STREAMP2P_API_KEY:
        return []
    rows: list[dict[str, Any]] = []
    page = 1
    while True:
        payload = _stream_get("/video/manage", params={"page": page, "perPage": 100})
        if payload is None:
            break
        data = payload.get("data") if isinstance(payload, dict) else None
        if data is None and isinstance(payload, list):
            data = payload
        if not data:
            break
        rows.extend(data)
        metadata = payload.get("metadata") if isinstance(payload, dict) else {}
        max_page = int(metadata.get("maxPage") or metadata.get("pages") or page)
        if page >= max_page:
            break
        page += 1
    return rows


def _sync_sort_key(item: tuple[Any, ...]) -> tuple[int, int, int, str]:
    is_movie, num, kind, _title, _url, server = item
    kind_order = {"HS": 0, "SS": 1, "DUB": 2}.get(kind, 9)
    server_order = 0 if server == "dood" else 1
    return (1 if is_movie else 0, int(num), kind_order * 10 + server_order, str(_title).lower())


def bulk_sync() -> int:
    parsed: list[tuple[Any, ...]] = []

    print("Fetching all DoodStream files...")
    dood_files = fetch_all_dood_files()
    print(f"  Found {len(dood_files)} DoodStream files")
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
            parsed.append((is_movie, num, kind, title, url, "dood"))

    if STREAMP2P_API_KEY:
        print("Fetching all StreamP2P files...")
        stream_files = fetch_all_streamp2p_files()
        print(f"  Found {len(stream_files)} StreamP2P files")
        for row in stream_files:
            title = (row.get("name") or row.get("title") or "").strip()
            match = TITLE_RE.search(title)
            if not match:
                continue
            is_movie = bool(match.group(1))
            num = int(match.group(2))
            kind = match.group(3).upper()
            video_id = _extract_stream_video_id(row) or ""
            url = _extract_stream_url(row) or _build_stream_url(video_id) or ""
            if url:
                parsed.append((is_movie, num, kind, title, url, "s2"))

    parsed.sort(key=_sync_sort_key)

    html = read_html()
    patched = 0
    for is_movie, num, kind, title, url, server in parsed:
        if is_movie:
            if kind == "HS":
                html = patch_movie_hs2(html, num, url) if server == "s2" else patch_movie_hs(html, num, url)
            elif kind in ("SS", "DUB"):
                html = patch_movie_ss2(html, num, url) if server == "s2" else patch_movie_ss(html, num, url)
        else:
            if kind == "HS":
                html = patch_hs2(html, num, url) if server == "s2" else patch_hs(html, num, url)
            elif kind in ("SS", "DUB"):
                html = patch_ss2(html, num, url) if server == "s2" else patch_ss(html, num, url)
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
    parser.add_argument("--hs", metavar="URL", help="Hard-sub DoodStream URL")
    parser.add_argument("--ss", metavar="URL", help="Soft-sub DoodStream URL")
    parser.add_argument("--hs2", metavar="URL", help="Hard-sub StreamP2P URL")
    parser.add_argument("--ss2", metavar="URL", help="Soft-sub StreamP2P URL")
    parser.add_argument("--bulk-sync", action="store_true", help="Sync all files from provider APIs")
    args = parser.parse_args()

    if args.bulk_sync:
        bulk_sync()
    elif args.ep is not None or args.movie is not None:
        apply_patch(ep=args.ep, movie=args.movie, hs_url=args.hs, ss_url=args.ss, hs2_url=args.hs2, ss2_url=args.ss2)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
