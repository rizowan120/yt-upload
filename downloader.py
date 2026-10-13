import asyncio
import os
import re
import shutil
import tempfile
import urllib.parse
from pathlib import Path
from typing import Optional, Callable, Awaitable

import aiohttp
from telethon import TelegramClient
from telethon.tl.types import DocumentAttributeFilename

from state import (
    ALLOWED_HOSTS,
    MAX_DOWNLOAD_GB,
    DOWNLOAD_DIR,
)

# Constants for Telegram download
PARALLEL_WORKERS = 16
PARALLEL_CHUNK = 512 * 1024  # 512KB
PARALLEL_THRESHOLD = 100 * 1024 * 1024  # 100 MB


# ======================================================================
#  Formatting helpers
# ======================================================================

def format_size(num_bytes: int) -> str:
    """Format bytes to human-readable string."""
    if num_bytes <= 0:
        return "Unknown size"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if num_bytes < 1024 or unit == "TB":
            return f"{num_bytes:.2f} {unit}" if unit in ["MB", "GB", "TB"] else f"{num_bytes} {unit}"
        num_bytes /= 1024
    return f"{num_bytes:.2f} GB"


def format_time(seconds: float) -> str:
    """Format seconds into human-readable duration."""
    if seconds < 0:
        return "—"
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    minutes, secs = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m {secs:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes:02d}m"


def format_speed(bps: float) -> str:
    """Format bytes per second into human-readable speed."""
    if bps <= 0:
        return "— MB/s"
    mbs = bps / (1024 * 1024)
    if mbs >= 1:
        return f"{mbs:.1f} MB/s"
    kbs = bps / 1024
    return f"{kbs:.0f} KB/s"


# ======================================================================
#  Title cleanup
# ======================================================================

def clean_title(filename: str) -> str:
    """
    Take the file name, remove the extension, replace dots and underscores with spaces,
    strip tags like 1080p, WEB-DL, x264, Dual Audio, and collapse spaces. Max 100 chars.
    """
    stem = Path(filename).stem
    text = stem.replace(".", " ").replace("_", " ")

    patterns = [
        r"\[.*?\]",
        r"\(.*?\)",
        r"\b(?:480p|720p|1080p|1440p|2160p|4k|8k)\b",
        r"\b(?:web[-\s]?dl|web[-\s]?rip|bluray|brrip|bdrip|hdrip|dvdrip|hdtv)\b",
        r"\b(?:x264|x265|h\.?264|h\.?265|hevc|avc|xvid)\b",
        r"\b(?:dual[-\s]?audio|multi[-\s]?audio)\b",
        r"\b(?:aac(?:\s*\d\s*\d)?|ac3|ddp?(?:\s*\d\s*\d)?|dts(?:-hd)?|truehd|atmos)\b",
        r"\b(?:10bit|8bit|hdr10\+?|hdr|sdr|uhd|remux|repack|proper)\b",
    ]
    for pat in patterns:
        text = re.sub(pat, " ", text, flags=re.IGNORECASE)

    # Remove release group suffix like -GroupName
    text = re.sub(r"\s*-\s*[a-zA-Z0-9]+$", "", text)
    # Remove leading/trailing dashes, colons, or punctuation
    text = re.sub(r"[\s\-_\–\—\:]+$", "", text)
    text = re.sub(r"^[\s\-_\–\—\:]+", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:100] or "Video"


# ======================================================================
#  URL and host validation
# ======================================================================

def is_host_allowed(url_str: str) -> tuple[bool, str]:
    """
    Validate that scheme is https and hostname EXACTLY equals an entry in ALLOWED_HOSTS.
    No substring or endswith checks.
    """
    try:
        parsed = urllib.parse.urlsplit(url_str)
        if parsed.scheme.lower() != "https":
            return False, "Scheme must be https"
        hostname = (parsed.hostname or "").lower()
        if not hostname:
            return False, "Missing hostname"
        if hostname not in ALLOWED_HOSTS:
            return False, f"Domain '{hostname}' is not allowed"
        return True, ""
    except Exception as e:
        return False, str(e)


def extract_url(text: str) -> Optional[str]:
    """Find the first http/https URL in a text string."""
    match = re.search(r"https?://[^\s<>\"']+", text)
    if match:
        return match.group(0)
    return None


def check_disk_space(required_bytes: int, target_dir: Optional[str] = None):
    """Ensure sufficient free disk space is available."""
    check_dir = target_dir or DOWNLOAD_DIR or tempfile.gettempdir()
    os.makedirs(check_dir, exist_ok=True)
    usage = shutil.disk_usage(check_dir)
    # Require at least required_bytes + 200MB margin
    margin = 200 * 1024 * 1024
    needed = required_bytes + margin
    if required_bytes > 0 and usage.free < needed:
        raise OSError(
            f"Insufficient disk space: {format_size(usage.free)} free, but {format_size(needed)} needed."
        )


def _extract_filename_from_headers(headers: dict, fallback_url: str) -> str:
    """Extract filename from Content-Disposition header or URL path."""
    cd = headers.get("Content-Disposition", "")
    if cd:
        m_star = re.search(r"filename\*\s*=\s*(?:UTF-8|utf-8)''([^;]+)", cd, re.IGNORECASE)
        if m_star:
            return urllib.parse.unquote(m_star.group(1))
        m = re.search(r'filename\s*=\s*(?:"([^"]+)"|([^;\s]+))', cd, re.IGNORECASE)
        if m:
            return m.group(1) or m.group(2)
    path = urllib.parse.urlsplit(fallback_url).path
    name = Path(urllib.parse.unquote(path)).name
    return name if name else "video.mp4"


# ======================================================================
#  Inspect URL (HEAD/GET with redirect validation)
# ======================================================================

async def inspect_url(url: str) -> tuple[str, str, int]:
    """
    Inspect URL with strict redirect checking.
    Returns (final_url, filename, file_size_in_bytes).
    Rejects if size > MAX_DOWNLOAD_GB or if disk space is insufficient.
    """
    allowed, err = is_host_allowed(url)
    if not allowed:
        raise ValueError(err)

    curr_url = url
    async with aiohttp.ClientSession() as session:
        # Follow redirects manually to validate every redirect host
        for _ in range(10):
            allowed, err = is_host_allowed(curr_url)
            if not allowed:
                raise ValueError(f"Redirect blocked: {err}")

            # Try HEAD first
            try:
                async with session.head(curr_url, allow_redirects=False) as resp:
                    if resp.status in (301, 302, 303, 307, 308):
                        loc = resp.headers.get("Location")
                        if not loc:
                            raise ValueError("Redirect without Location header")
                        curr_url = urllib.parse.urljoin(curr_url, loc)
                        continue
                    if resp.status == 200:
                        size = int(resp.headers.get("Content-Length", 0))
                        fname = _extract_filename_from_headers(resp.headers, curr_url)
                        _validate_size_and_disk(size)
                        return curr_url, fname, size
            except (aiohttp.ClientError, asyncio.TimeoutError):
                pass

            # If HEAD returned non-200 or failed, fallback to GET (headers only)
            async with session.get(
                curr_url, headers={"Range": "bytes=0-0"}, allow_redirects=False
            ) as resp:
                if resp.status in (301, 302, 303, 307, 308):
                    loc = resp.headers.get("Location")
                    if not loc:
                        raise ValueError("Redirect without Location header")
                    curr_url = urllib.parse.urljoin(curr_url, loc)
                    continue

                if resp.status in (200, 206):
                    size = 0
                    if resp.status == 206 and "Content-Range" in resp.headers:
                        m = re.search(r"/(\d+)", resp.headers["Content-Range"])
                        if m:
                            size = int(m.group(1))
                    if size == 0:
                        size = int(resp.headers.get("Content-Length", 0))
                    fname = _extract_filename_from_headers(resp.headers, curr_url)
                    _validate_size_and_disk(size)
                    return curr_url, fname, size
                else:
                    raise ValueError(f"HTTP server returned status {resp.status}")

        raise ValueError("Too many redirects")


def _validate_size_and_disk(size: int):
    max_bytes = int(MAX_DOWNLOAD_GB * 1024 * 1024 * 1024)
    if size > max_bytes:
        raise ValueError(
            f"File size ({format_size(size)}) exceeds maximum allowed ({MAX_DOWNLOAD_GB} GB)"
        )
    check_disk_space(size)


# ======================================================================
#  Download via aiohttp with HTTP Range resume
# ======================================================================

async def download_url_stream(
    url: str,
    dest_path: str,
    total_size: int,
    progress_cb: Callable[[int, int], Awaitable[None]],
    cancel_event: asyncio.Event,
    max_retries: int = 5,
) -> str:
    """
    Download a URL stream in chunks with HTTP Range resume up to max_retries times.
    Re-validates host on every redirect.
    """
    retries = 0
    timeout = aiohttp.ClientTimeout(total=None, sock_read=60, sock_connect=30)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        while retries < max_retries:
            if cancel_event.is_set():
                raise asyncio.CancelledError("Download cancelled")

            current_bytes = os.path.getsize(dest_path) if os.path.exists(dest_path) else 0
            if total_size > 0 and current_bytes >= total_size:
                return dest_path

            headers = {}
            if current_bytes > 0:
                headers["Range"] = f"bytes={current_bytes}-"

            try:
                curr_url = url
                # Handle redirects manually
                for _ in range(10):
                    allowed, err = is_host_allowed(curr_url)
                    if not allowed:
                        raise ValueError(f"Disallowed host on redirect: {err}")

                    async with session.get(curr_url, headers=headers, allow_redirects=False) as resp:
                        if resp.status in (301, 302, 303, 307, 308):
                            loc = resp.headers.get("Location")
                            if not loc:
                                raise ValueError("Redirect missing Location header")
                            curr_url = urllib.parse.urljoin(curr_url, loc)
                            continue

                        if resp.status == 416:
                            # Range Not Satisfiable: file might already be complete
                            if total_size > 0 and current_bytes >= total_size:
                                return dest_path
                            current_bytes = 0
                            headers.pop("Range", None)
                            continue

                        if resp.status not in (200, 206):
                            raise aiohttp.ClientResponseError(
                                resp.request_info,
                                resp.history,
                                status=resp.status,
                                message=f"HTTP {resp.status}",
                            )

                        mode = "ab" if resp.status == 206 else "wb"
                        if resp.status == 200:
                            current_bytes = 0

                        if total_size <= 0:
                            cl = resp.headers.get("Content-Length")
                            if cl and cl.isdigit():
                                total_size = current_bytes + int(cl)

                        with open(dest_path, mode) as f:
                            async for chunk in resp.content.iter_chunked(1024 * 1024):
                                if cancel_event.is_set():
                                    raise asyncio.CancelledError("Download cancelled")
                                f.write(chunk)
                                current_bytes += len(chunk)
                                if progress_cb:
                                    await progress_cb(current_bytes, total_size)

                        return dest_path

                raise ValueError("Too many redirects during download")

            except asyncio.CancelledError:
                raise
            except Exception as e:
                retries += 1
                if retries >= max_retries:
                    raise e
                await asyncio.sleep(2)

    return dest_path


# ======================================================================
#  Download direct Telegram video
# ======================================================================

async def download_tg_video(
    tg_client: TelegramClient,
    event,
    dest_path: str,
    total_size: int,
    progress_cb: Callable[[int, int], Awaitable[None]],
    cancel_event: asyncio.Event,
) -> str:
    """Download video sent directly in Telegram, supporting parallel chunks for large files."""
    use_parallel = event.document and total_size > PARALLEL_THRESHOLD

    if use_parallel:
        from telethon.utils import get_input_location

        dc_id, input_location = get_input_location(event.document)
        chunk_size = PARALLEL_CHUNK
        workers = min(PARALLEL_WORKERS, max(2, total_size // (50 * 1024 * 1024)))
        stride = workers * chunk_size

        progress_lock = asyncio.Lock()
        worker_bytes = [0] * workers

        with open(dest_path, "wb") as f:
            if total_size > 0:
                f.seek(total_size - 1)
                f.write(b"\0")

        async def _worker(worker_id: int):
            offset = worker_id * chunk_size
            bytes_downloaded = 0
            iteration = 0
            with open(dest_path, "r+b") as f:
                async for chunk in tg_client.iter_download(
                    event.document,
                    offset=offset,
                    stride=stride,
                    request_size=chunk_size,
                    file_size=total_size,
                    dc_id=dc_id,
                ):
                    if cancel_event.is_set():
                        raise asyncio.CancelledError("Download cancelled")
                    write_offset = offset + iteration * stride
                    f.seek(write_offset)
                    f.write(chunk)

                    bytes_downloaded += len(chunk)
                    iteration += 1

                    if progress_cb:
                        async with progress_lock:
                            worker_bytes[worker_id] = bytes_downloaded
                            total_done = sum(worker_bytes)
                            await progress_cb(total_done, total_size)

        tasks = [asyncio.create_task(_worker(i)) for i in range(workers)]
        try:
            await asyncio.gather(*tasks)
        except Exception:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
    else:
        async def _tg_cb(received, total):
            if cancel_event.is_set():
                raise asyncio.CancelledError("Download cancelled")
            if progress_cb:
                await progress_cb(received, total or total_size)

        await event.download_media(file=dest_path, progress_callback=_tg_cb)

    return dest_path
