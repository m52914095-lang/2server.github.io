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

import base64
import glob
import json
import mimetypes
import os
import re
import subprocess
import sys
import time
from datetime import datetime
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from update import bulk_sync, patch_hs, patch_movie_hs, patch_movie_ss, patch_ss, read_html, write_html

# Config
DOODSTREAM_API_KEY = os.environ.get("DOODSTREAM_API_KEY", "554366xrjxeza9m7e4m02v")
HARD_SUB_FOLDER_ID = os.environ.get("HARD_SUB_FOLDER_ID", "")
SOFT_SUB_FOLDER_ID = os.environ.get("SOFT_SUB_FOLDER_ID", "")
STREAMP2P_API_KEY = os.environ.get("STREAMP2P_API_KEY", "").strip()
STREAMP2P_PLAYER_URL = os.environ.get("STREAMP2P_PLAYER_URL", "").strip()
STREAMP2P_FOLDER_ID = os.environ.get("STREAMP2P_FOLDER_ID", "").strip()
STREAMP2P_ENABLED = os.environ.get("STREAMP2P_ENABLED", "1").strip() != "0" and bool(STREAMP2P_API_KEY)

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
STREAMP2P_CHUNK_SIZE = 52_428_800
STREAMP2P_POLL_SECONDS = 15
STREAMP2P_POLL_TIMEOUT = 1800

VIDEO_EXTENSIONS = {".mkv", ".mp4", ".avi", ".m4v", ".mov"}
SUBTITLE_EXTENSIONS = {".ass", ".ssa", ".srt", ".vtt", ".sub", ".sup"}
ZIP_EXTENSIONS = {".zip"}
ENGLISH_TAGS = {"eng", "en", "english"}

_upload_server_url: str | None = None
_streamp2p_auth_checked = False


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



# StreamP2P upload helpers

def guess_video_mime(file_path: str) -> str:
    ext = os.path.splitext(file_path)[1].lower()
    if ext == ".mp4":
        return "video/mp4"
    if ext == ".mkv":
        return "video/x-matroska"
    if ext == ".mov":
        return "video/quicktime"
    if ext == ".avi":
        return "video/x-msvideo"
    if ext == ".m4v":
        return "video/x-m4v"
    guessed = mimetypes.guess_type(file_path)[0]
    return guessed or "application/octet-stream"


def _b64_tus(value: str) -> str:
    return base64.b64encode(value.encode("utf-8")).decode("ascii")


def streamp2p_headers() -> dict[str, str]:
    token = STREAMP2P_API_KEY.strip().strip('"').strip("'")
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    return {"api-token": token, "Accept": "application/json"}


def streamp2p_auth_test() -> bool:
    global _streamp2p_auth_checked
    if not STREAMP2P_ENABLED:
        return False
    if _streamp2p_auth_checked:
        return True

    try:
        response = requests.get(
            "https://streamp2p.com/api/v1/user/information",
            headers=streamp2p_headers(),
            timeout=30,
        )
        preview = response.text[:200].replace("\n", " ")
        print(f"  StreamP2P auth test: {response.status_code} {preview}")
        response.raise_for_status()
        _streamp2p_auth_checked = True
        return True
    except Exception as exc:
        print(f"  StreamP2P auth failed: {exc}", file=sys.stderr)
        return False


def get_streamp2p_upload_target() -> tuple[str | None, str | None]:
    try:
        response = requests.get(
            "https://streamp2p.com/api/v1/video/upload",
            headers=streamp2p_headers(),
            timeout=60,
        )
        response.raise_for_status()
        data = response.json()
        tus_url = data.get("tusUrl") or (data.get("result") or {}).get("tusUrl")
        access_token = data.get("accessToken") or (data.get("result") or {}).get("accessToken")
        if tus_url and access_token:
            return str(tus_url), str(access_token)
        print(f"  StreamP2P upload target response missing fields: {data}", file=sys.stderr)
    except Exception as exc:
        print(f"  StreamP2P GET /video/upload failed: {exc}", file=sys.stderr)
    return None, None


def _tus_metadata(access_token: str, filename: str, filetype: str, folder_id: str = "") -> str:
    entries = [
        f"accessToken {_b64_tus(access_token)}",
        f"filename {_b64_tus(filename)}",
        f"filetype {_b64_tus(filetype)}",
    ]
    if folder_id:
        entries.append(f"folderId {_b64_tus(folder_id)}")
    return ",".join(entries)


def _create_tus_upload(tus_url: str, access_token: str, upload_name: str, filetype: str, file_size: int, folder_id: str = "") -> str | None:
    headers = {
        "Tus-Resumable": "1.0.0",
        "Upload-Length": str(file_size),
        "Upload-Metadata": _tus_metadata(access_token, upload_name, filetype, folder_id),
        "Content-Length": "0",
    }
    response = requests.post(tus_url, headers=headers, timeout=60)
    response.raise_for_status()
    location = response.headers.get("Location") or response.headers.get("location")
    if not location:
        print(f"  StreamP2P TUS create missing Location header: {dict(response.headers)}", file=sys.stderr)
        return None
    return urljoin(tus_url, location)


def _patch_tus_chunks(upload_url: str, file_path: str) -> bool:
    offset = 0
    file_size = os.path.getsize(file_path)
    with open(file_path, "rb") as fh:
        while offset < file_size:
            chunk = fh.read(STREAMP2P_CHUNK_SIZE)
            if not chunk:
                break
            headers = {
                "Tus-Resumable": "1.0.0",
                "Upload-Offset": str(offset),
                "Content-Type": "application/offset+octet-stream",
            }
            response = requests.patch(upload_url, headers=headers, data=chunk, timeout=ARIA2_TIMEOUT)
            response.raise_for_status()
            server_offset = response.headers.get("Upload-Offset") or response.headers.get("upload-offset")
            if server_offset is not None:
                offset = int(server_offset)
            else:
                offset += len(chunk)
            print(f"    StreamP2P uploaded {offset}/{file_size} bytes")

    verify = requests.head(upload_url, headers={"Tus-Resumable": "1.0.0"}, timeout=30)
    verify.raise_for_status()
    final_offset = int(verify.headers.get("Upload-Offset") or verify.headers.get("upload-offset") or "0")
    if final_offset != file_size:
        print(f"  StreamP2P upload verification mismatch: {final_offset} != {file_size}", file=sys.stderr)
        return False
    return True


def _extract_rows_from_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if not isinstance(payload, dict):
        return []

    for key in ("results", "items", "rows", "videos", "data", "list"):
        value = payload.get(key)
        if isinstance(value, list):
            return [row for row in value if isinstance(row, dict)]
        if isinstance(value, dict):
            nested = _extract_rows_from_payload(value)
            if nested:
                return nested

    for value in payload.values():
        if isinstance(value, dict):
            nested = _extract_rows_from_payload(value)
            if nested:
                return nested
        if isinstance(value, list) and value and all(isinstance(row, dict) for row in value):
            return list(value)
    return []


def _row_title(row: dict[str, Any]) -> str:
    return str(row.get("title") or row.get("name") or row.get("filename") or row.get("originalName") or "").strip()


def _row_video_id(row: dict[str, Any]) -> str:
    for key in ("id", "videoId", "_id"):
        value = row.get(key)
        if value:
            return str(value)
    return ""


def _row_player_url(row: dict[str, Any]) -> str:
    for key in ("playerUrl", "embedUrl", "iframeUrl", "url", "publicUrl", "shareUrl"):
        value = row.get(key)
        if value:
            return str(value)
    return ""


def _build_player_url(video_id: str) -> str:
    base = STREAMP2P_PLAYER_URL.strip()
    if not base or not video_id:
        return ""
    if "#" in base:
        return f"{base}{video_id}"
    return f"{base.rstrip('/')}/#{video_id}"


def find_streamp2p_video(title: str, upload_name: str, timeout_seconds: int = STREAMP2P_POLL_TIMEOUT) -> tuple[str | None, str | None]:
    deadline = time.time() + timeout_seconds
    title_lower = title.lower().strip()
    upload_stem = os.path.splitext(os.path.basename(upload_name))[0].lower().strip()

    while time.time() < deadline:
        for query in (title, upload_name, upload_stem):
            if not query:
                continue
            try:
                response = requests.get(
                    "https://streamp2p.com/api/v1/video/manage",
                    headers=streamp2p_headers(),
                    params={"search": query, "page": 1, "perPage": 100},
                    timeout=60,
                )
                response.raise_for_status()
                rows = _extract_rows_from_payload(response.json())
            except Exception as exc:
                print(f"  StreamP2P list search failed for '{query}': {exc}", file=sys.stderr)
                rows = []

            exact_match = None
            partial_match = None
            for row in rows:
                row_title = _row_title(row).lower()
                if not row_title:
                    continue
                if row_title == title_lower or row_title == upload_stem:
                    exact_match = row
                    break
                if title_lower in row_title or upload_stem in row_title:
                    partial_match = partial_match or row
            chosen = exact_match or partial_match
            if chosen:
                video_id = _row_video_id(chosen)
                player_url = _row_player_url(chosen) or _build_player_url(video_id)
                return video_id or None, player_url or None

        print("  Waiting for StreamP2P video to appear in /video/manage...")
        time.sleep(STREAMP2P_POLL_SECONDS)

    return None, None


def upload_streamp2p_subtitle(video_id: str, subtitle_file: str) -> bool:
    if not video_id or not subtitle_file or not os.path.exists(subtitle_file):
        return False
    try:
        mime = mimetypes.guess_type(subtitle_file)[0] or "application/octet-stream"
        with open(subtitle_file, "rb") as fh:
            response = requests.put(
                f"https://streamp2p.com/api/v1/video/manage/{video_id}/subtitle",
                headers={"api-token": streamp2p_headers()["api-token"]},
                files={"file": (os.path.basename(subtitle_file), fh, mime)},
                timeout=ARIA2_TIMEOUT,
            )
        if response.ok:
            print(f"  StreamP2P subtitle uploaded for video {video_id}")
            return True
        print(f"  StreamP2P subtitle upload failed: {response.status_code} {response.text[:300]}", file=sys.stderr)
    except Exception as exc:
        print(f"  StreamP2P subtitle upload exception: {exc}", file=sys.stderr)
    return False


def upload_file_streamp2p(file_path: str, title: str, subtitle_file: str | None = None) -> str | None:
    if not STREAMP2P_ENABLED:
        return None
    if not streamp2p_auth_test():
        return None

    file_size = os.path.getsize(file_path)
    upload_name = f"{title}{os.path.splitext(file_path)[1].lower()}"
    filetype = guess_video_mime(file_path)
    print(f"  StreamP2P uploading '{upload_name}' ({file_size // (1024 * 1024)} MB)...")

    for attempt in range(1, UPLOAD_RETRIES + 1):
        try:
            tus_url, access_token = get_streamp2p_upload_target()
            if not tus_url or not access_token:
                print(f"  [attempt {attempt}] StreamP2P did not return upload targets", file=sys.stderr)
                raise RuntimeError("missing StreamP2P upload targets")

            upload_url = _create_tus_upload(tus_url, access_token, upload_name, filetype, file_size, STREAMP2P_FOLDER_ID)
            if not upload_url:
                raise RuntimeError("could not create StreamP2P tus upload")

            if not _patch_tus_chunks(upload_url, file_path):
                raise RuntimeError("StreamP2P tus verification failed")

            video_id, player_url = find_streamp2p_video(title, upload_name)
            if subtitle_file and video_id:
                upload_streamp2p_subtitle(video_id, subtitle_file)

            if player_url:
                print(f"  StreamP2P uploaded! {player_url}")
            elif video_id:
                print(f"  StreamP2P uploaded! video id: {video_id}")
            else:
                print("  StreamP2P upload finished, but no player URL was resolved yet")
            return player_url or video_id
        except Exception as exc:
            print(f"  [attempt {attempt}] StreamP2P exception: {exc}", file=sys.stderr)
            if attempt < UPLOAD_RETRIES:
                print(f"  Retrying StreamP2P in {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)

    print(f"  All {UPLOAD_RETRIES} StreamP2P attempts failed for '{title}'", file=sys.stderr)
    return None

# Per-file processing

def process_file(video_file: str, subtitle_files: list[str]) -> tuple[int, bool, str | None, str | None]:
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

    hs_url = None
    ss_url = None
    ss_file = None
    hs_file = None

    try:
        ss_file = remux_to_mp4(video_file, label)
        if ss_file:
            title = MOVIE_SS_TITLE_TPL.format(num=number) if is_movie else SS_TITLE_TPL.format(ep=number)
            ss_url = upload_file(ss_file, title, SOFT_SUB_FOLDER_ID)
            upload_file_streamp2p(ss_file, title, matched_subtitle)
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
            hs_url = upload_file(hs_file, title, HARD_SUB_FOLDER_ID)
            upload_file_streamp2p(hs_file, title, None)
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

    return number, is_movie, hs_url, ss_url


# HTML patching and git helpers

def patch_html_batch(results: list[tuple[int, bool, str | None, str | None]]) -> bool:
    if not any(hs or ss for _, _, hs, ss in results):
        print("\nNo URLs obtained - index.html unchanged.")
        return False

    html = read_html()
    for number, is_movie, hs_url, ss_url in results:
        if is_movie:
            if hs_url:
                html = patch_movie_hs(html, number, hs_url)
            if ss_url:
                html = patch_movie_ss(html, number, ss_url)
        else:
            if hs_url:
                html = patch_hs(html, number, hs_url)
            if ss_url:
                html = patch_ss(html, number, ss_url)
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


def git_commit_push(results: list[tuple[int, bool, str | None, str | None]], sync_only: bool = False) -> None:
    if not git_has_changes():
        print("\n  No HTML changes to commit.")
        return

    ep_parts = [str(number) for number, is_movie, hs, ss in results if not is_movie and (hs or ss)]
    movie_parts = [f"M{number}" for number, is_movie, hs, ss in results if is_movie and (hs or ss)]

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

def run_auto_sync(results: list[tuple[int, bool, str | None, str | None]]) -> None:
    try:
        print("\nRunning update.py bulk sync...")
        bulk_sync()
    except Exception as exc:
        print(f"  Bulk sync error: {exc}", file=sys.stderr)
    git_commit_push(results, sync_only=not bool(results))


def main() -> None:
    subtitle_files: list[str] = []
    all_videos: list[str] = []

    if STREAMP2P_ENABLED:
        streamp2p_auth_test()

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
    results: list[tuple[int, bool, str | None, str | None]] = []
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
    for number, is_movie, hs_url, ss_url in results:
        kind = "Movie" if is_movie else "EP"
        hs = "OK" if hs_url else "FAIL"
        ss = "OK" if ss_url else "FAIL"
        print(f"  {kind} {number:>4}  SS:{ss}  HS:{hs}")

    failed = [number for number, _is_movie, hs, ss in results if not hs and not ss]
    if failed:
        print(f"\n  {len(failed)} fully failed: {failed}")
        sys.exit(1)

    print(f"\n  All {len(results)} done.")


if __name__ == "__main__":
    main()
