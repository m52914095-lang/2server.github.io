"""
conan_automation_github.py - Detective Conan automated downloader + uploader.

Added features:
- separate subtitle magnet support
- external subtitle matching by episode or movie number
- embedded English subtitle auto-selection via ffprobe
- SELECT_FILES support for aria2c
- 6 Nyaa search strategies with full-site seeded fallback
- DHT + PEX + LPD for aria2c
- bulk auto sync via update.py
- numeric git sorting and logged git pull --rebase
- ASCII-only source file
"""

import glob
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime
from typing import Any

import requests
from bs4 import BeautifulSoup

from update import (
    bulk_sync,
    patch_hs,
    patch_hs2,
    patch_movie_hs,
    patch_movie_hs2,
    patch_movie_ss,
    patch_movie_ss2,
    patch_ss,
    patch_ss2,
    read_html,
    write_html,
)

# Config
DOODSTREAM_API_KEY = os.environ.get("DOODSTREAM_API_KEY", "").strip()
HARD_SUB_FOLDER_ID = os.environ.get("HARD_SUB_FOLDER_ID", "")
SOFT_SUB_FOLDER_ID = os.environ.get("SOFT_SUB_FOLDER_ID", "")
STREAMP2P_API_KEY = os.environ.get("STREAMP2P_API_KEY", "").strip()
STREAMP2P_PLAYER_URL = os.environ.get("STREAMP2P_PLAYER_URL", "").strip()

BASE_EPISODE = int(os.environ.get("BASE_EPISODE", "1193"))
BASE_DATE = os.environ.get("BASE_DATE", "2026-03-14")

EPISODE_OVERRIDE = os.environ.get("EPISODE_OVERRIDE", "").strip()
MAGNET_LINKS = os.environ.get("MAGNET_LINKS", "").strip()
SUBTITLE_MAGNET_LINKS = os.environ.get("SUBTITLE_MAGNET_LINKS", "").strip()
SELECT_FILES = os.environ.get("SELECT_FILES", "").strip()
SUBTITLE_SELECT_FILES = os.environ.get("SUBTITLE_SELECT_FILES", "").strip() or SELECT_FILES
CUSTOM_SEARCH = os.environ.get("CUSTOM_SEARCH", "").strip()
NYAA_UPLOADER_URL = os.environ.get("NYAA_UPLOADER_URL", "").strip()

MOVIE_MODE = os.environ.get("MOVIE_MODE", "0").strip() == "1"

HS_TITLE_TPL = os.environ.get("HS_TITLE_TPL", "Detective Conan - {ep} HS")
SS_TITLE_TPL = os.environ.get("SS_TITLE_TPL", "Detective Conan - {ep} SS")
MOVIE_HS_TITLE_TPL = os.environ.get("MOVIE_HS_TITLE_TPL", "Detective Conan Movie - {num} HS")
MOVIE_SS_TITLE_TPL = os.environ.get("MOVIE_SS_TITLE_TPL", "Detective Conan Movie - {num} SS")

HTML_FILE = os.environ.get("HTML_FILE", "index.html")

UPLOAD_RETRIES = 3
RETRY_DELAY = 10
ARIA2_TIMEOUT = 7200

VIDEO_EXTENSIONS = {".mkv", ".mp4", ".avi", ".m4v", ".mov"}
SUBTITLE_EXTENSIONS = {".ass", ".ssa", ".srt", ".vtt", ".sub", ".sup"}
ZIP_EXTENSIONS = {".zip"}
ENGLISH_TAGS = {"eng", "en", "english"}

STREAM_API_BASE = "https://streamp2p.com/api/v1"
STREAM_POLL_DELAY = 8
STREAM_POLL_ATTEMPTS = 75
STREAM_HTTP_TIMEOUT = 60

_upload_server_url: str | None = None
_stream_player_base_url: str | None = None


# Parsing helpers

def parse_file_info(filename: str) -> tuple[int | None, bool]:
    """Return (number, is_movie) from a file name."""
    base = os.path.basename(filename)
    lower = base.lower()

    if MOVIE_MODE:
        match = re.search(r"\bmovie\s*[-\u2013]?\s*(\d{1,3})\b", base, re.IGNORECASE)
        if not match:
            match = re.search(r"\b(\d{1,3})\b", base)
        return (int(match.group(1)) if match else None, True)

    if re.search(r"\b(movie|film|ova|special)\b", lower):
        match = re.search(r"\b(?:movie|film|ova|special)\s*[-\u2013]?\s*(\d{1,3})\b", base, re.IGNORECASE)
        if not match:
            match = re.search(r"\b(\d{1,3})\b", base)
        return (int(match.group(1)) if match else None, True)

    patterns = [
        r"Detective Conan\s*[-\u2013]\s*(\d{3,4})\b",
        r"Case Closed\s*[-\u2013]?\s*(\d{3,4})\b",
        r"\b(?:ep|episode|e)\s*(\d{3,4})\b",
        r"\[(\d{3,4})\]",
        r"\b(\d{3,4})\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, base, re.IGNORECASE)
        if match:
            return int(match.group(1)), False

    return None, False


def get_auto_episode() -> int:
    base_dt = datetime.strptime(BASE_DATE, "%Y-%m-%d")
    weeks = max(0, (datetime.now() - base_dt).days // 7)
    return BASE_EPISODE + weeks


def parse_episode_override(raw: str) -> list[int]:
    raw = raw.strip()
    if not raw:
        return [get_auto_episode()]

    episodes: list[int] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            left, right = part.split("-", 1)
            try:
                start = int(left.strip())
                end = int(right.strip())
            except ValueError:
                print(f"  WARNING: could not parse range '{part}' - skipping", file=sys.stderr)
                continue
            if start > end:
                start, end = end, start
            episodes.extend(range(start, end + 1))
        else:
            try:
                episodes.append(int(part))
            except ValueError:
                print(f"  WARNING: could not parse episode '{part}' - skipping", file=sys.stderr)

    if not episodes:
        return [get_auto_episode()]

    seen: set[int] = set()
    unique: list[int] = []
    for ep in episodes:
        if ep not in seen:
            seen.add(ep)
            unique.append(ep)
    return unique


def parse_magnet_list(raw: str) -> list[str]:
    normalized = raw.replace(",magnet:", "\nmagnet:")
    lines = []
    for line in normalized.splitlines():
        line = line.strip()
        if line:
            lines.append(line)
    return [line for line in lines if line.startswith("magnet:")]


def validate_select_files(raw: str) -> str:
    raw = raw.strip()
    if not raw:
        return ""
    if not re.fullmatch(r"[0-9,\- ]+", raw):
        print(f"  WARNING: invalid SELECT_FILES '{raw}' - ignoring", file=sys.stderr)
        return ""
    return raw.replace(" ", "")


# Nyaa search

def _build_nyaa_urls(episode: int) -> list[tuple[str, str]]:
    queries: list[str] = []
    if CUSTOM_SEARCH:
        queries.append(CUSTOM_SEARCH)
    queries.extend([
        f"Detective Conan - {episode} 1080p",
        f"Detective Conan {episode} 1080p",
        f"Meitantei Conan {episode} 1080p",
        f"Case Closed {episode} 1080p",
        f"Detective Conan - {episode}",
        f"Detective Conan {episode}",
    ])

    seen: set[tuple[str, str]] = set()
    urls: list[tuple[str, str]] = []
    uploader_base = NYAA_UPLOADER_URL.rstrip("/") if NYAA_UPLOADER_URL else ""

    for index, query in enumerate(queries[:6], start=1):
        encoded = requests.utils.quote(query)
        for category, label in (("1_2", f"strategy {index}"), ("0_0", f"strategy {index} full-site")):
            if uploader_base:
                url = f"{uploader_base}?f=0&c={category}&q={encoded}&s=seeders&o=desc"
            else:
                url = f"https://nyaa.si/?f=0&c={category}&q={encoded}&s=seeders&o=desc"
            key = (label, url)
            if key not in seen:
                seen.add(key)
                urls.append(key)
    return urls


def _extract_seeders(cells: list[str]) -> int:
    numeric = []
    for cell in cells:
        text = cell.replace(",", "").strip()
        if re.fullmatch(r"\d+", text):
            numeric.append(int(text))
    if len(numeric) >= 3:
        return numeric[-3]
    if numeric:
        return numeric[-1]
    return 0


def _score_nyaa_result(title: str, episode: int, seeds: int, strategy_idx: int) -> tuple[int, int, int]:
    lower = title.lower()
    score = 0
    if re.search(rf"(?<!\d){episode}(?!\d)", title):
        score += 1000
    if "detective conan" in lower or "meitantei conan" in lower or "case closed" in lower:
        score += 200
    if "1080p" in lower:
        score += 100
    if "subsplease" in lower:
        score += 50
    if any(bad in lower for bad in ("batch", "multi-audio")):
        score -= 25
    score -= strategy_idx * 10
    return score, seeds, -strategy_idx


def search_nyaa(episode: int) -> str | None:
    candidates: list[dict[str, Any]] = []
    for strategy_idx, (label, url) in enumerate(_build_nyaa_urls(episode), start=1):
        print(f"  Searching Nyaa ({label}): {url}")
        try:
            response = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
            response.raise_for_status()
        except Exception as exc:
            print(f"  Nyaa error ({label}): {exc}", file=sys.stderr)
            continue

        soup = BeautifulSoup(response.text, "html.parser")
        rows = soup.select("tr.success, tr.default, tr.danger")
        for row in rows:
            title_tag = row.select_one("a[title]")
            title = (title_tag.get("title") if title_tag else "") or (title_tag.get_text(" ", strip=True) if title_tag else "")
            if not title:
                links = row.find_all("a", href=True)
                title = links[1].get_text(" ", strip=True) if len(links) > 1 else ""
            magnet = None
            for link in row.find_all("a", href=True):
                if link["href"].startswith("magnet:"):
                    magnet = link["href"]
                    break
            if not title or not magnet:
                continue

            cells = [cell.get_text(" ", strip=True) for cell in row.find_all("td")]
            seeds = _extract_seeders(cells)
            score = _score_nyaa_result(title, episode, seeds, strategy_idx)
            candidates.append({
                "title": title,
                "magnet": magnet,
                "seeds": seeds,
                "score": score,
            })

        strict_matches = [c for c in candidates if re.search(rf"(?<!\d){episode}(?!\d)", c["title"])]
        if strict_matches:
            strict_matches.sort(key=lambda item: (item["score"], item["seeds"]), reverse=True)
            best = strict_matches[0]
            print(f"  Best match: {best['title']} | seeds={best['seeds']}")
            return best["magnet"]

    if candidates:
        candidates.sort(key=lambda item: (item["score"], item["seeds"]), reverse=True)
        best = candidates[0]
        print(f"  Fallback match: {best['title']} | seeds={best['seeds']}")
        return best["magnet"]

    return None


# Download helpers

def _snapshot_by_extension(extensions: set[str]) -> set[str]:
    found: set[str] = set()
    for path in glob.glob("**/*", recursive=True):
        if not os.path.isfile(path):
            continue
        ext = os.path.splitext(path)[1].lower()
        if ext in extensions:
            found.add(os.path.normpath(path))
    return found


def _extract_zip_subtitles(paths: list[str]) -> list[str]:
    extracted: list[str] = []
    for archive in paths:
        if os.path.splitext(archive)[1].lower() not in ZIP_EXTENSIONS:
            continue
        out_dir = os.path.splitext(archive)[0] + "_unzipped"
        os.makedirs(out_dir, exist_ok=True)
        try:
            subprocess.run(["unzip", "-o", archive, "-d", out_dir], check=True, capture_output=True, text=True, timeout=600)
        except Exception as exc:
            print(f"  Could not unzip subtitle archive '{archive}': {exc}", file=sys.stderr)
            continue
        for path in glob.glob(os.path.join(out_dir, "**", "*"), recursive=True):
            if os.path.isfile(path) and os.path.splitext(path)[1].lower() in SUBTITLE_EXTENSIONS:
                extracted.append(os.path.normpath(path))
    return extracted


def download_magnet(magnet: str, select_files: str = "", wanted_extensions: set[str] | None = None) -> list[str]:
    if wanted_extensions is None:
        wanted_extensions = VIDEO_EXTENSIONS

    before = _snapshot_by_extension(wanted_extensions | ZIP_EXTENSIONS)
    print(f"  Downloading: {magnet[:90]}...")

    cmd = [
        "aria2c",
        "--seed-time=0",
        "--max-connection-per-server=4",
        "--split=4",
        "--file-allocation=none",
        "--bt-stop-timeout=300",
        "--enable-dht=true",
        "--enable-peer-exchange=true",
        "--bt-enable-lpd=true",
        "--continue=true",
        "--follow-torrent=true",
        magnet,
    ]
    select_value = validate_select_files(select_files)
    if select_value:
        cmd.insert(-1, f"--select-file={select_value}")
        print(f"  Using SELECT_FILES={select_value}")

    try:
        subprocess.run(cmd, check=True, timeout=ARIA2_TIMEOUT)
    except subprocess.TimeoutExpired:
        print("  aria2c timeout - checking partial files", file=sys.stderr)
    except subprocess.CalledProcessError as exc:
        print(f"  aria2c error: {exc}", file=sys.stderr)

    after = _snapshot_by_extension(wanted_extensions | ZIP_EXTENSIONS)
    new_paths = sorted(after - before, key=lambda item: os.path.getmtime(item))

    subtitle_archives = [path for path in new_paths if os.path.splitext(path)[1].lower() in ZIP_EXTENSIONS]
    if subtitle_archives and wanted_extensions == SUBTITLE_EXTENSIONS:
        new_paths.extend(_extract_zip_subtitles(subtitle_archives))

    new_files = [path for path in new_paths if os.path.splitext(path)[1].lower() in wanted_extensions]
    print(f"  New files: {new_files or 'none'}")
    return new_files


# Subtitle helpers

def _subtitle_score(path: str, number: int, is_movie: bool) -> tuple[int, int]:
    base = os.path.basename(path).lower()
    score = 0
    parsed_number, parsed_movie = parse_file_info(path)
    if parsed_number == number:
        score += 100
    if parsed_movie == is_movie:
        score += 20
    if any(tag in base for tag in ("english", " eng ", ".eng.", "_eng", "[eng]", "en")):
        score += 40
    ext = os.path.splitext(path)[1].lower()
    if ext == ".ass":
        score += 10
    elif ext == ".ssa":
        score += 8
    elif ext == ".srt":
        score += 6
    return score, int(os.path.getmtime(path))


def find_matching_external_subtitle(video_file: str, subtitle_files: list[str]) -> str | None:
    number, is_movie = parse_file_info(video_file)
    if number is None:
        return None

    candidates = []
    for subtitle in subtitle_files:
        sub_num, sub_movie = parse_file_info(subtitle)
        if sub_num == number and sub_movie == is_movie:
            candidates.append(subtitle)
        elif sub_num == number:
            candidates.append(subtitle)

    if not candidates:
        return None

    candidates.sort(key=lambda path: _subtitle_score(path, number, is_movie), reverse=True)
    return candidates[0]


def get_embedded_english_subtitle_index(input_file: str) -> int | None:
    cmd = [
        "ffprobe",
        "-v", "error",
        "-select_streams", "s",
        "-show_entries", "stream=index:stream_tags=language,title",
        "-of", "json",
        input_file,
    ]
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=120)
    except Exception as exc:
        print(f"  ffprobe subtitle scan failed: {exc}", file=sys.stderr)
        return None

    try:
        data = json.loads(result.stdout)
    except Exception:
        return None

    streams = data.get("streams") or []
    if not streams:
        return None

    best_index = None
    best_score = -1
    for subtitle_pos, stream in enumerate(streams):
        tags = stream.get("tags") or {}
        language = str(tags.get("language") or "").strip().lower()
        title = str(tags.get("title") or "").strip().lower()
        score = 0
        if language in ENGLISH_TAGS:
            score += 100
        if "english" in title or "eng" in title:
            score += 50
        if score > best_score:
            best_score = score
            best_index = subtitle_pos

    if best_score < 0:
        return 0
    return best_index


# ffmpeg helpers

def _esc(path: str) -> str:
    value = path.replace("\\", "\\\\").replace("'", "\\'")
    return value.replace(":", "\\:").replace("[", "\\[").replace("]", "\\]")


def _remux_ok(path: str) -> bool:
    return os.path.exists(path) and os.path.getsize(path) > 10 * 1024 * 1024


def remux_to_mp4(input_file: str, label: str) -> str | None:
    output = f"conan_{label}_ss.mp4"
    if os.path.exists(output):
        os.remove(output)

    print(f"  Remuxing MKV -> MP4 for SS -> {output}")

    attempts = [
        ("video+audio stream copy", ["-c:v", "copy", "-c:a", "copy"]),
        ("video copy + audio re-encode AAC", ["-c:v", "copy", "-c:a", "aac", "-b:a", "192k"]),
        (
            "full re-encode H.264 + AAC",
            ["-c:v", "libx264", "-preset", "veryfast", "-crf", "22", "-c:a", "aac", "-b:a", "192k"],
        ),
    ]

    for desc, codec_flags in attempts:
        if os.path.exists(output):
            os.remove(output)
        cmd = [
            "ffmpeg", "-y", "-i", input_file,
            *codec_flags,
            "-sn",
            "-movflags", "+faststart",
            output,
        ]
        print(f"  Remux attempt ({desc})...")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=ARIA2_TIMEOUT)
        if result.returncode == 0 and _remux_ok(output):
            size_mb = os.path.getsize(output) // (1024 * 1024)
            print(f"  Remux OK ({size_mb} MB): {output}")
            return output
        print(f"  Remux failed [{desc}] rc={result.returncode}", file=sys.stderr)
        if result.stderr:
            print(f"  {result.stderr[-600:]}", file=sys.stderr)

    print(f"  All 3 remux attempts failed for {input_file}", file=sys.stderr)
    return None


def hardsub(input_file: str, label: str, external_subtitle: str | None = None) -> str | None:
    output = f"conan_{label}_hs.mp4"
    print(f"  Hard-subbing -> {output}")

    filters: list[str] = []
    if external_subtitle:
        print(f"  Using external subtitle: {external_subtitle}")
        filters = [f"subtitles='{_esc(external_subtitle)}'", f"subtitles={_esc(external_subtitle)}"]
    else:
        subtitle_index = get_embedded_english_subtitle_index(input_file)
        if subtitle_index is not None:
            print(f"  Using embedded subtitle stream index: {subtitle_index}")
            filters = [
                f"subtitles='{_esc(input_file)}':si={subtitle_index}",
                f"subtitles={_esc(input_file)}:si={subtitle_index}",
            ]
        else:
            filters = [f"subtitles='{_esc(input_file)}'", f"subtitles={_esc(input_file)}"]

    for vf in filters:
        cmd = [
            "ffmpeg", "-y", "-i", input_file,
            "-vf", vf,
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "22",
            "-c:a", "aac", "-b:a", "192k",
            output,
        ]
        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=ARIA2_TIMEOUT)
            print(f"  Hard-sub complete: {output}")
            return output
        except subprocess.CalledProcessError as exc:
            print(f"  ffmpeg attempt failed:\n{exc.stderr[-600:]}", file=sys.stderr)

    print(f"  Hard-sub FAILED for {label}", file=sys.stderr)
    return None


# DoodStream upload helpers

def get_upload_server() -> str | None:
    global _upload_server_url
    if _upload_server_url:
        return _upload_server_url
    try:
        resp = requests.get(
            "https://doodapi.co/api/upload/server",
            params={"key": DOODSTREAM_API_KEY},
            timeout=20,
        ).json()
        if resp.get("status") == 200:
            _upload_server_url = resp["result"]
            return _upload_server_url
    except Exception as exc:
        print(f"  Upload server error: {exc}", file=sys.stderr)
    return None


def rename_dood_file(file_code: str, title: str) -> None:
    try:
        resp = requests.get(
            "https://doodapi.co/api/file/rename",
            params={"key": DOODSTREAM_API_KEY, "file_code": file_code, "title": title},
            timeout=15,
        ).json()
        if resp.get("status") == 200:
            print(f"  Title set: '{title}'")
        else:
            print(f"  Rename API returned: {resp}", file=sys.stderr)
    except Exception as exc:
        print(f"  Rename API error: {exc}", file=sys.stderr)


def upload_file(file_path: str, title: str, folder_id: str = "") -> str | None:
    size_mb = os.path.getsize(file_path) // (1024 * 1024)
    print(f"  Uploading '{title}' ({size_mb} MB)...")

    for attempt in range(1, UPLOAD_RETRIES + 1):
        global _upload_server_url
        _upload_server_url = None
        server = get_upload_server()
        if not server:
            print(f"  [attempt {attempt}] No upload server", file=sys.stderr)
            time.sleep(RETRY_DELAY)
            continue

        try:
            with open(file_path, "rb") as fh:
                data = {"api_key": DOODSTREAM_API_KEY}
                if folder_id:
                    data["fld_id"] = folder_id
                resp = requests.post(
                    server,
                    files={"file": (os.path.basename(file_path), fh, "video/mp4")},
                    data=data,
                    timeout=ARIA2_TIMEOUT,
                ).json()

            if resp.get("status") == 200:
                result = resp["result"][0]
                file_code = result.get("file_code") or result.get("filecode") or ""
                url = result.get("download_url") or result.get("embed_url") or ""
                if file_code:
                    rename_dood_file(file_code, title)
                print(f"  Uploaded! {url}")
                return url
            print(f"  [attempt {attempt}] Bad response: {resp}", file=sys.stderr)
        except Exception as exc:
            print(f"  [attempt {attempt}] Exception: {exc}", file=sys.stderr)

        if attempt < UPLOAD_RETRIES:
            print(f"  Retrying in {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)

    print(f"  All {UPLOAD_RETRIES} attempts failed for '{title}'", file=sys.stderr)
    return None



# StreamP2P helpers

def streamp2p_enabled() -> bool:
    return bool(STREAMP2P_API_KEY)


def _streamp2p_headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {STREAMP2P_API_KEY}",
        "Accept": "application/json",
    }


def _normalize_player_base_url(value: str | None) -> str | None:
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    if not raw.startswith(("http://", "https://")):
        raw = "https://" + raw.lstrip("/")
    try:
        parsed = requests.utils.urlparse(raw)
    except Exception:
        return None
    if not parsed.netloc:
        return None
    path = parsed.path.rstrip("/")
    return f"{parsed.scheme}://{parsed.netloc}{path}"


def _walk_json(value: Any):
    if isinstance(value, dict):
        yield value
        for item in value.values():
            yield from _walk_json(item)
    elif isinstance(value, list):
        for item in value:
            yield from _walk_json(item)


def _collect_url_candidates(value: Any) -> list[str]:
    found: list[str] = []
    for item in _walk_json(value):
        if isinstance(item, dict):
            for candidate in item.values():
                if isinstance(candidate, str) and candidate.startswith(("http://", "https://")):
                    found.append(candidate)
        elif isinstance(item, str) and item.startswith(("http://", "https://")):
            found.append(item)
    seen: set[str] = set()
    ordered: list[str] = []
    for candidate in found:
        if candidate not in seen:
            seen.add(candidate)
            ordered.append(candidate)
    return ordered


def _collect_named_ids(value: Any) -> dict[str, list[str]]:
    found: dict[str, list[str]] = {}
    for item in _walk_json(value):
        if not isinstance(item, dict):
            continue
        for key, candidate in item.items():
            if key.lower() in {
                "id",
                "videoid",
                "video_id",
                "taskid",
                "task_id",
                "uploadid",
                "upload_id",
            }:
                if candidate is None:
                    continue
                found.setdefault(key.lower(), []).append(str(candidate))
        videos = item.get("videos")
        if isinstance(videos, list):
            for candidate in videos:
                if candidate is not None:
                    found.setdefault("videos", []).append(str(candidate))
    return found


def _extract_streamp2p_upload_targets(payload: Any) -> list[dict[str, Any]]:
    targets: list[dict[str, Any]] = []
    for item in _walk_json(payload):
        if not isinstance(item, dict):
            continue
        url = None
        for key in ("url", "uploadUrl", "uploadURL", "endpoint", "uploadEndpoint", "server"):
            candidate = item.get(key)
            if isinstance(candidate, str) and candidate.startswith(("http://", "https://")):
                url = candidate
                break
        if not url:
            continue
        target = {
            "url": url,
            "method": str(item.get("method") or "POST").upper(),
            "fields": item.get("fields") if isinstance(item.get("fields"), dict) else {},
            "headers": item.get("headers") if isinstance(item.get("headers"), dict) else {},
            "file_field": item.get("fileField") or item.get("file_field") or "file",
        }
        targets.append(target)

    if not targets:
        for url in _collect_url_candidates(payload):
            if "/upload" in url:
                targets.append({"url": url, "method": "POST", "fields": {}, "headers": {}, "file_field": "file"})

    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for target in targets:
        key = (target["method"], target["url"])
        if key not in seen:
            seen.add(key)
            deduped.append(target)

    def _target_sort_key(item: dict[str, Any]) -> tuple[int, int, str]:
        url = item["url"]
        is_api = 1 if "/api/v1/video/upload" in url else 0
        is_cloudflare = 1 if "streamp2p.com/api/" in url else 0
        return (is_api + is_cloudflare, 0 if item["method"] == "POST" else 1, url)

    return sorted(deduped, key=_target_sort_key)


def _streamp2p_request(method: str, path: str, timeout: int = STREAM_HTTP_TIMEOUT, **kwargs) -> requests.Response:
    headers = _streamp2p_headers()
    extra_headers = kwargs.pop("headers", {}) or {}
    headers.update(extra_headers)
    return requests.request(method, f"{STREAM_API_BASE}{path}", headers=headers, timeout=timeout, **kwargs)


def _streamp2p_get_json(path: str, timeout: int = STREAM_HTTP_TIMEOUT) -> Any | None:
    try:
        response = _streamp2p_request("GET", path, timeout=timeout)
        response.raise_for_status()
        return response.json()
    except Exception as exc:
        print(f"  StreamP2P GET {path} failed: {exc}", file=sys.stderr)
        return None


def _get_streamp2p_player_base() -> str | None:
    global _stream_player_base_url
    if _stream_player_base_url:
        return _stream_player_base_url
    if STREAMP2P_PLAYER_URL:
        _stream_player_base_url = _normalize_player_base_url(STREAMP2P_PLAYER_URL)
        if _stream_player_base_url:
            return _stream_player_base_url

    payload = _streamp2p_get_json("/video/player/default")
    if payload is None:
        return None

    for url in _collect_url_candidates(payload):
        normalized = _normalize_player_base_url(url)
        if normalized:
            _stream_player_base_url = normalized
            return _stream_player_base_url

    for item in _walk_json(payload):
        if not isinstance(item, dict):
            continue
        for key in ("domain", "hostname", "host", "subdomain"):
            candidate = item.get(key)
            if not isinstance(candidate, str):
                continue
            if "." in candidate and " " not in candidate:
                normalized = _normalize_player_base_url(candidate)
                if normalized:
                    _stream_player_base_url = normalized
                    return _stream_player_base_url
    return None


def _build_streamp2p_embed_url(video_id: str) -> str | None:
    base = _get_streamp2p_player_base()
    if not base or not video_id:
        return None
    if "#" in base:
        return base.split("#", 1)[0] + f"#{video_id}"
    return base.rstrip("/") + f"/#{video_id}".replace("/#", "#")


def _extract_streamp2p_play_url(payload: Any, fallback_video_id: str = "") -> str | None:
    urls = _collect_url_candidates(payload)
    preferred: list[str] = []
    fallback: list[str] = []
    for url in urls:
        lower = url.lower()
        if any(token in lower for token in ("playerp2p.com", "streamp2p.com/e/", "embed", "/play", "#")):
            preferred.append(url)
        else:
            fallback.append(url)
    if preferred:
        return preferred[0]
    if fallback_video_id:
        built = _build_streamp2p_embed_url(fallback_video_id)
        if built:
            return built
    if fallback:
        return fallback[0]
    return None


def _streamp2p_rename_video(video_id: str, title: str) -> None:
    if not video_id or not title:
        return
    try:
        response = _streamp2p_request("PATCH", f"/video/manage/{video_id}", json={"name": title})
        if response.status_code not in (200, 204):
            print(f"  StreamP2P rename returned {response.status_code}: {response.text[:200]}", file=sys.stderr)
    except Exception as exc:
        print(f"  StreamP2P rename error: {exc}", file=sys.stderr)


def _streamp2p_video_detail(video_id: str) -> Any | None:
    if not video_id:
        return None
    return _streamp2p_get_json(f"/video/manage/{video_id}")


def _streamp2p_resolve_video_id_from_task(task_id: str) -> str | None:
    if not task_id:
        return None
    for attempt in range(1, STREAM_POLL_ATTEMPTS + 1):
        payload = _streamp2p_get_json(f"/video/advance-upload/{task_id}")
        if not payload:
            time.sleep(STREAM_POLL_DELAY)
            continue
        ids = _collect_named_ids(payload)
        videos = ids.get("videos") or []
        if videos:
            return videos[0]
        status = str((payload or {}).get("status") or "").strip().lower()
        error_message = str((payload or {}).get("error") or "").strip()
        if error_message:
            print(f"  StreamP2P task error: {error_message}", file=sys.stderr)
            return None
        if status in {"completed", "done", "ready", "success"}:
            generic_id = (ids.get("videoid") or ids.get("video_id") or ids.get("id") or [None])[0]
            if generic_id:
                return generic_id
        print(f"  StreamP2P task {task_id}: waiting ({attempt}/{STREAM_POLL_ATTEMPTS}) status='{status or 'pending'}'")
        time.sleep(STREAM_POLL_DELAY)
    return None


def _streamp2p_finalize_upload(payload: Any, title: str) -> tuple[str | None, str | None]:
    if payload is None:
        return None, None

    video_id = None
    ids = _collect_named_ids(payload)
    if ids.get("videos"):
        video_id = ids["videos"][0]
    elif ids.get("videoid"):
        video_id = ids["videoid"][0]
    elif ids.get("video_id"):
        video_id = ids["video_id"][0]

    task_id = None
    if ids.get("taskid"):
        task_id = ids["taskid"][0]
    elif ids.get("task_id"):
        task_id = ids["task_id"][0]
    elif ids.get("uploadid"):
        task_id = ids["uploadid"][0]
    elif ids.get("upload_id"):
        task_id = ids["upload_id"][0]

    generic_id = (ids.get("id") or [None])[0]
    direct_url = _extract_streamp2p_play_url(payload, video_id or "")
    if direct_url and video_id:
        _streamp2p_rename_video(video_id, title)
        return direct_url, video_id

    if not video_id and task_id:
        video_id = _streamp2p_resolve_video_id_from_task(task_id)
    if not video_id and generic_id:
        maybe_task_video = _streamp2p_resolve_video_id_from_task(generic_id)
        if maybe_task_video:
            video_id = maybe_task_video
        else:
            video_id = generic_id

    if video_id:
        _streamp2p_rename_video(video_id, title)
        detail = _streamp2p_video_detail(video_id)
        detail_url = _extract_streamp2p_play_url(detail, video_id)
        if detail_url:
            return detail_url, video_id
        built_url = _build_streamp2p_embed_url(video_id)
        if built_url:
            return built_url, video_id

    return direct_url, video_id


def _streamp2p_request_upload_targets() -> list[dict[str, Any]]:
    if not streamp2p_enabled():
        return []
    payload = _streamp2p_get_json("/video/upload")
    if payload is None:
        return []
    targets = _extract_streamp2p_upload_targets(payload)
    if not targets:
        print("  StreamP2P upload endpoint list was empty", file=sys.stderr)
    else:
        print(f"  StreamP2P upload target count: {len(targets)}")
    return targets


def _upload_to_streamp2p_target(file_path: str, target: dict[str, Any], title: str) -> tuple[str | None, str | None]:
    file_name = os.path.basename(file_path)
    mime_type = "video/mp4" if file_name.lower().endswith(".mp4") else "application/octet-stream"
    method = str(target.get("method") or "POST").upper()
    url = str(target.get("url") or "")
    data = dict(target.get("fields") or {})
    headers = dict(target.get("headers") or {})
    file_field = str(target.get("file_field") or "file")

    if not url:
        return None, None

    with open(file_path, "rb") as fh:
        if method == "PUT" and not data:
            upload_headers = dict(headers)
            upload_headers.setdefault("Content-Type", mime_type)
            response = requests.put(url, data=fh, headers=upload_headers, timeout=ARIA2_TIMEOUT)
        else:
            files = {file_field: (file_name, fh, mime_type)}
            response = requests.request(method, url, data=data, files=files, headers=headers, timeout=ARIA2_TIMEOUT)

    payload = None
    try:
        payload = response.json()
    except Exception:
        snippet = (response.text or "").strip()
        if snippet:
            print(f"  StreamP2P non-JSON response: {snippet[:400]}", file=sys.stderr)

    if response.status_code not in (200, 201, 202, 204):
        print(f"  StreamP2P bad response ({response.status_code}): {payload or {}}", file=sys.stderr)
        return None, None

    if payload is None:
        payload = {"location": response.headers.get("Location") or response.headers.get("location") or ""}

    return _streamp2p_finalize_upload(payload, title)


def upload_streamp2p_file(file_path: str, title: str) -> tuple[str | None, str | None]:
    if not streamp2p_enabled():
        return None, None

    size_mb = os.path.getsize(file_path) // (1024 * 1024)
    print(f"  StreamP2P uploading '{title}' ({size_mb} MB)...")

    for attempt in range(1, UPLOAD_RETRIES + 1):
        targets = _streamp2p_request_upload_targets()
        if not targets:
            print(f"  [attempt {attempt}] StreamP2P did not return upload targets", file=sys.stderr)
            if attempt < UPLOAD_RETRIES:
                time.sleep(RETRY_DELAY)
            continue

        for target in targets:
            print(f"  [attempt {attempt}] StreamP2P target: {target['url']} ({target['method']})")
            try:
                url, video_id = _upload_to_streamp2p_target(file_path, target, title)
                if url or video_id:
                    resolved_url = url or (_build_streamp2p_embed_url(video_id or "") if video_id else None)
                    if resolved_url:
                        print(f"  StreamP2P uploaded! {resolved_url}")
                        return resolved_url, video_id
            except Exception as exc:
                print(f"  [attempt {attempt}] StreamP2P exception: {exc}", file=sys.stderr)

        if attempt < UPLOAD_RETRIES:
            print(f"  StreamP2P retrying in {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)

    print(f"  All {UPLOAD_RETRIES} StreamP2P attempts failed for '{title}'", file=sys.stderr)
    return None, None


def upload_streamp2p_subtitle(video_id: str, subtitle_path: str | None) -> None:
    if not streamp2p_enabled() or not video_id or not subtitle_path or not os.path.exists(subtitle_path):
        return
    ext = os.path.splitext(subtitle_path)[1].lower().lstrip(".") or "srt"
    data = {"language": "en", "label": "English", "type": ext}
    try:
        with open(subtitle_path, "rb") as fh:
            response = _streamp2p_request(
                "PUT",
                f"/video/manage/{video_id}/subtitle",
                files={"file": (os.path.basename(subtitle_path), fh, "application/octet-stream")},
                data=data,
                timeout=ARIA2_TIMEOUT,
            )
        if response.status_code in (200, 201, 202, 204):
            print(f"  StreamP2P subtitle attached: {os.path.basename(subtitle_path)}")
        else:
            print(f"  StreamP2P subtitle upload returned {response.status_code}: {response.text[:300]}", file=sys.stderr)
    except Exception as exc:
        print(f"  StreamP2P subtitle upload error: {exc}", file=sys.stderr)


# Per-file processing

def process_file(video_file: str, subtitle_files: list[str]) -> dict[str, Any]:
    number, is_movie = parse_file_info(video_file)
    if number is None:
        number = get_auto_episode()
        is_movie = MOVIE_MODE
        print(f"  Could not parse from filename - using calculated EP {number}")
    else:
        kind = "Movie" if is_movie else "Episode"
        print(f"  Auto-detected: {kind} {number} ({os.path.basename(video_file)})")

    label = f"m{number}" if is_movie else str(number)
    matched_subtitle = find_matching_external_subtitle(video_file, subtitle_files)
    if matched_subtitle:
        print(f"  Matched external subtitle by number: {matched_subtitle}")
    else:
        print("  No external subtitle match found; will use embedded subtitles if available")

    result: dict[str, Any] = {
        "number": number,
        "is_movie": is_movie,
        "dood_hs": None,
        "dood_ss": None,
        "s2_hs": None,
        "s2_ss": None,
    }

    ss_file = None
    hs_file = None

    try:
        ss_file = remux_to_mp4(video_file, label)
        if ss_file:
            title = MOVIE_SS_TITLE_TPL.format(num=number) if is_movie else SS_TITLE_TPL.format(ep=number)
            result["dood_ss"] = upload_file(ss_file, title, SOFT_SUB_FOLDER_ID)
            s2_url, s2_video_id = upload_streamp2p_file(ss_file, title)
            result["s2_ss"] = s2_url
            if s2_video_id and matched_subtitle:
                upload_streamp2p_subtitle(s2_video_id, matched_subtitle)
        else:
            print("  Remux failed - skipping SS upload", file=sys.stderr)
    except Exception as exc:
        print(f"  SS exception: {exc}", file=sys.stderr)
    finally:
        if ss_file and os.path.exists(ss_file):
            try:
                os.remove(ss_file)
            except OSError:
                pass

    try:
        hs_file = hardsub(video_file, label, matched_subtitle)
        if hs_file:
            title = MOVIE_HS_TITLE_TPL.format(num=number) if is_movie else HS_TITLE_TPL.format(ep=number)
            result["dood_hs"] = upload_file(hs_file, title, HARD_SUB_FOLDER_ID)
            s2_url, _s2_video_id = upload_streamp2p_file(hs_file, title)
            result["s2_hs"] = s2_url
    except Exception as exc:
        print(f"  HS exception: {exc}", file=sys.stderr)
    finally:
        if hs_file and os.path.exists(hs_file):
            try:
                os.remove(hs_file)
            except OSError:
                pass

    try:
        os.remove(video_file)
    except OSError:
        pass

    return result


# HTML patching and git helpers

def patch_html_batch(results: list[dict[str, Any]]) -> bool:
    if not any(result.get(key) for result in results for key in ("dood_hs", "dood_ss", "s2_hs", "s2_ss")):
        print("\nNo URLs obtained - index.html unchanged.")
        return False

    html = read_html()
    for result in results:
        number = int(result["number"])
        is_movie = bool(result["is_movie"])
        if is_movie:
            if result.get("dood_hs"):
                html = patch_movie_hs(html, number, str(result["dood_hs"]))
            if result.get("dood_ss"):
                html = patch_movie_ss(html, number, str(result["dood_ss"]))
            if result.get("s2_hs"):
                html = patch_movie_hs2(html, number, str(result["s2_hs"]))
            if result.get("s2_ss"):
                html = patch_movie_ss2(html, number, str(result["s2_ss"]))
        else:
            if result.get("dood_hs"):
                html = patch_hs(html, number, str(result["dood_hs"]))
            if result.get("dood_ss"):
                html = patch_ss(html, number, str(result["dood_ss"]))
            if result.get("s2_hs"):
                html = patch_hs2(html, number, str(result["s2_hs"]))
            if result.get("s2_ss"):
                html = patch_ss2(html, number, str(result["s2_ss"]))
    write_html(html)
    return True


def git_has_changes() -> bool:
    result = subprocess.run(["git", "status", "--porcelain", HTML_FILE], capture_output=True, text=True, check=False)
    return bool(result.stdout.strip())


def _run_logged(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    print(f"  $ {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.stdout.strip():
        print(result.stdout.strip())
    if result.stderr.strip():
        print(result.stderr.strip(), file=sys.stderr)
    if check and result.returncode != 0:
        raise subprocess.CalledProcessError(result.returncode, cmd, result.stdout, result.stderr)
    return result


def git_commit_push(results: list[dict[str, Any]], sync_only: bool = False) -> None:
    if not git_has_changes():
        print("\n  No HTML changes to commit.")
        return

    ep_parts = [str(result["number"]) for result in results if not result["is_movie"] and any(result.get(key) for key in ("dood_hs", "dood_ss", "s2_hs", "s2_ss"))]
    movie_parts = [f"M{result['number']}" for result in results if result["is_movie"] and any(result.get(key) for key in ("dood_hs", "dood_ss", "s2_hs", "s2_ss"))]

    ep_parts = sorted(set(ep_parts), key=int)
    movie_parts = sorted(set(movie_parts), key=lambda item: int(item[1:]))

    if ep_parts or movie_parts:
        label = ", ".join(ep_parts + movie_parts)
        message = f"chore: add links for {label}"
    else:
        message = "chore: auto sync index.html"
        if sync_only:
            print("  Commit message: auto sync index.html")

    try:
        _run_logged(["git", "config", "user.email", "github-actions@github.com"])
        _run_logged(["git", "config", "user.name", "GitHub Actions"])
        _run_logged(["git", "add", HTML_FILE])
        commit_result = _run_logged(["git", "commit", "-m", message], check=False)
        if commit_result.returncode != 0:
            print("  Git commit skipped - nothing new to commit")
            return
        print("  Running git pull --rebase with logging...")
        _run_logged(["git", "pull", "--rebase"], check=False)
        _run_logged(["git", "push"])
        print(f"\n  Git pushed: {message}")
    except subprocess.CalledProcessError as exc:
        print(f"  Git error: {exc}", file=sys.stderr)


# Main

def run_auto_sync(results: list[dict[str, Any]]) -> None:
    try:
        print("\nRunning update.py bulk sync...")
        bulk_sync()
    except Exception as exc:
        print(f"  Bulk sync error: {exc}", file=sys.stderr)
    git_commit_push(results, sync_only=not bool(results))


def main() -> None:
    subtitle_files: list[str] = []
    all_videos: list[str] = []

    if SUBTITLE_MAGNET_LINKS:
        subtitle_magnets = parse_magnet_list(SUBTITLE_MAGNET_LINKS)
        print(f"Subtitle magnet mode: {len(subtitle_magnets)} magnet(s)")
        for index, magnet in enumerate(subtitle_magnets, start=1):
            print(f"\n[SUB {index}/{len(subtitle_magnets)}] Downloading subtitle magnet...")
            new_subs = download_magnet(magnet, SUBTITLE_SELECT_FILES, SUBTITLE_EXTENSIONS)
            subtitle_files.extend(new_subs)
        subtitle_files = sorted(set(subtitle_files), key=lambda item: os.path.basename(item).lower())
        print(f"  Total subtitle files ready: {len(subtitle_files)}")

    if MAGNET_LINKS:
        magnets = parse_magnet_list(MAGNET_LINKS)
        print(f"Batch mode: {len(magnets)} magnet(s) | Movie mode: {MOVIE_MODE}")
        for index, magnet in enumerate(magnets, start=1):
            print(f"\n[{index}/{len(magnets)}] Downloading video magnet...")
            new_files = download_magnet(magnet, SELECT_FILES, VIDEO_EXTENSIONS)
            if not new_files:
                print("  No video files found - skipping", file=sys.stderr)
            else:
                all_videos.extend(new_files)
    else:
        episodes = parse_episode_override(EPISODE_OVERRIDE)
        if len(episodes) == 1 and not EPISODE_OVERRIDE.strip():
            print(f"Auto mode - episode {episodes[0]} (calculated) | Movie mode: {MOVIE_MODE}")
        else:
            print(f"Episode mode - {len(episodes)} episode(s): {episodes} | Movie mode: {MOVIE_MODE}")

        not_found = []
        for ep in episodes:
            print(f"\n  Searching for episode {ep}...")
            magnet = search_nyaa(ep)
            if not magnet:
                print(f"  Episode {ep} not found on Nyaa - skipping", file=sys.stderr)
                not_found.append(ep)
                continue
            new_files = download_magnet(magnet, SELECT_FILES, VIDEO_EXTENSIONS)
            if not new_files:
                print(f"  No video files downloaded for episode {ep}", file=sys.stderr)
            else:
                all_videos.extend(new_files)

        if not_found:
            print(f"\n  Episodes not found on Nyaa: {not_found}", file=sys.stderr)

    all_videos = sorted(set(all_videos), key=lambda item: os.path.getmtime(item))
    if not all_videos:
        print("No files downloaded. Running auto sync only.")
        run_auto_sync([])
        sys.exit(0)

    print(f"\nProcessing {len(all_videos)} file(s)...")
    results: list[dict[str, Any]] = []
    for index, video in enumerate(all_videos, start=1):
        print(f"\n[{index}/{len(all_videos)}] {os.path.basename(video)}")
        try:
            results.append(process_file(video, subtitle_files))
        except Exception as exc:
            print(f"  FATAL ERROR: {exc}", file=sys.stderr)

    if results:
        patch_html_batch(results)

    run_auto_sync(results)

    print("\n-- Run summary --")
    require_s2 = streamp2p_enabled()
    failed: list[str] = []
    for result in results:
        number = int(result["number"])
        kind = "Movie" if result["is_movie"] else "EP"
        d1_ss = "OK" if result.get("dood_ss") else "FAIL"
        d1_hs = "OK" if result.get("dood_hs") else "FAIL"
        s2_ss = "OK" if result.get("s2_ss") else ("SKIP" if not require_s2 else "FAIL")
        s2_hs = "OK" if result.get("s2_hs") else ("SKIP" if not require_s2 else "FAIL")
        print(f"  {kind} {number:>4}  D1-SS:{d1_ss}  D1-HS:{d1_hs}  S2-SS:{s2_ss}  S2-HS:{s2_hs}")

        missing = []
        if not result.get("dood_ss") or (require_s2 and not result.get("s2_ss")):
            missing.append("SS")
        if not result.get("dood_hs") or (require_s2 and not result.get("s2_hs")):
            missing.append("HS")
        if missing:
            failed.append(f"{kind} {number} missing {'/'.join(missing)}")

    if failed:
        print(f"\n  {len(failed)} incomplete item(s): {failed}")
        sys.exit(1)

    print(f"\n  All {len(results)} done.")


if __name__ == "__main__":
    main()
