import asyncio
import json
import logging
import os
import re
import tempfile
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from telethon import TelegramClient, events
from telethon.tl.types import DocumentAttributeFilename

# ---- Load .env ----
load_dotenv()

# ---- Logging ----
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("ytbot")

# ---- Config ----
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
OWNER_ID = int(os.environ["OWNER_ID"])
TOKEN_FILE = "token.json"
SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]
HISTORY_FILE = "upload_history.json"

# ---- Per-user state (single owner) ----
privacy = os.environ.get("PRIVACY", "private")  # private | unlisted | public
default_tags: list[str] = []
default_category: str = "22"  # "People & Blogs"
default_playlist: Optional[str] = None  # playlist ID
pending_thumbnail: dict = {}  # chat_id -> video_id waiting for thumbnail
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

CATEGORY_MAP = {
    "1": "Film & Animation", "2": "Autos & Vehicles",
    "10": "Music", "15": "Pets & Animals",
    "17": "Sports", "18": "Short Movies",
    "19": "Travel & Events", "20": "Gaming",
    "21": "Videoblogging", "22": "People & Blogs",
    "23": "Comedy", "24": "Entertainment",
    "25": "News & Politics", "26": "Howto & Style",
    "27": "Education", "28": "Science & Technology",
    "29": "Nonprofits & Activism",
}

client = TelegramClient("ytbot", API_ID, API_HASH)


# ======================================================================
#  YouTube credentials
# ======================================================================

def get_credentials() -> Credentials:
    """Load and refresh YouTube OAuth credentials."""
    if not Path(TOKEN_FILE).exists():
        raise FileNotFoundError(
            f"{TOKEN_FILE} not found. Run auth.py or auth_manual.py first."
        )
    creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if not creds.valid:
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            Path(TOKEN_FILE).write_text(creds.to_json())
            log.info("Refreshed YouTube token")
        else:
            raise RuntimeError("YouTube token is invalid and cannot be refreshed.")
    return creds


def get_youtube():
    """Build an authenticated YouTube API service."""
    return build("youtube", "v3", credentials=get_credentials())


# ======================================================================
#  Upload history
# ======================================================================

def save_history(entry: dict):
    """Append an upload record to the history file."""
    history = []
    if Path(HISTORY_FILE).exists():
        try:
            history = json.loads(Path(HISTORY_FILE).read_text())
        except (json.JSONDecodeError, OSError):
            pass
    history.append(entry)
    Path(HISTORY_FILE).write_text(json.dumps(history, indent=2))


# ======================================================================
#  Upload queue
# ======================================================================

@dataclass
class QueueItem:
    event: object
    title: str
    description: str
    tags: list[str] = field(default_factory=list)
    category: str = "22"
    playlist_id: Optional[str] = None


upload_queue: deque[QueueItem] = deque()
queue_lock = asyncio.Lock()
queue_running = False


async def process_queue():
    """Process queued uploads one at a time."""
    global queue_running
    async with queue_lock:
        if queue_running:
            return
        queue_running = True

    try:
        while upload_queue:
            item = upload_queue.popleft()
            await _do_upload(item)
    finally:
        async with queue_lock:
            queue_running = False


# ======================================================================
#  Caption parsing
# ======================================================================

def parse_caption(raw_text: str) -> tuple[str, str, list[str]]:
    """
    Parse the message caption.
      - Line 1 → title
      - Remaining lines → description
      - Lines starting with # are extracted as tags
    Returns (title, description, tags).
    """
    text = (raw_text or "").strip()
    if not text:
        title = f"Upload {datetime.now():%Y-%m-%d %H:%M}"
        return title, "", []

    lines = text.split("\n")
    title = lines[0].strip()
    rest = "\n".join(lines[1:]).strip()

    # Extract #hashtags as tags
    tag_pattern = re.compile(r"#(\w+)")
    inline_tags = tag_pattern.findall(rest)
    # Remove hashtag lines from description
    desc_lines = [l for l in rest.split("\n") if not l.strip().startswith("#")]
    description = "\n".join(desc_lines).strip()

    return title[:100], description, inline_tags


# ======================================================================
#  Progress helpers
# ======================================================================

class ProgressTracker:
    """Throttled progress updater for Telegram status messages."""

    def __init__(self, status_msg, prefix: str, min_interval: float = 3.0):
        self.status_msg = status_msg
        self.prefix = prefix
        self.min_interval = min_interval
        self._last_update = 0.0
        self._last_text = ""
        self._start_time: Optional[float] = None
        self._samples: list[tuple[float, int]] = []  # (timestamp, bytes) for speed calc

    @staticmethod
    def _fmt_time(seconds: float) -> str:
        """Format seconds into a human-readable string."""
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

    @staticmethod
    def _fmt_speed(bps: float) -> str:
        """Format bytes/sec into a human-readable string."""
        if bps <= 0:
            return "— MB/s"
        mbs = bps / (1024 * 1024)
        if mbs >= 1:
            return f"{mbs:.1f} MB/s"
        kbs = bps / 1024
        return f"{kbs:.0f} KB/s"

    async def update(self, current: int, total: int):
        now = time.time()

        # Initialise start time on first call
        if self._start_time is None:
            self._start_time = now

        # Keep a rolling window of samples (last 10s) for speed calc
        self._samples.append((now, current))
        cutoff = now - 10.0
        self._samples = [(t, b) for t, b in self._samples if t >= cutoff]

        # Throttle edits
        if now - self._last_update < self.min_interval:
            return
        if total <= 0:
            return

        pct = min(current / total * 100, 100)
        bar_len = 14
        filled = int(bar_len * pct / 100)
        bar = "█" * filled + "░" * (bar_len - filled)
        mb_done = current / (1024 * 1024)
        mb_total = total / (1024 * 1024)

        elapsed = now - self._start_time

        # Speed from rolling window
        if len(self._samples) >= 2:
            dt = self._samples[-1][0] - self._samples[0][0]
            db = self._samples[-1][1] - self._samples[0][1]
            speed = db / dt if dt > 0 else 0
        else:
            speed = current / elapsed if elapsed > 0 else 0

        # ETA
        remaining_bytes = total - current
        eta = remaining_bytes / speed if speed > 0 else -1

        text = (
            f"{self.prefix}\n"
            f"{bar} {pct:.0f}%\n"
            f"{mb_done:.1f} / {mb_total:.1f} MB\n"
            f"Elapsed: {self._fmt_time(elapsed)}\n"
            f"ETA: {self._fmt_time(eta)}\n"
            f"Speed: {self._fmt_speed(speed)}"
        )

        if text == self._last_text:
            return
        try:
            await self.status_msg.edit(text)
            self._last_text = text
            self._last_update = now
        except Exception:
            pass  # ignore flood waits / edit-unchanged errors


def upload_to_youtube_with_progress(
    path: str,
    title: str,
    description: str,
    tags: list[str],
    category: str,
    progress_callback=None,
) -> str:
    """Upload a video to YouTube with chunked progress reporting."""
    youtube = get_youtube()

    all_tags = list(set(tags + default_tags))

    body = {
        "snippet": {
            "title": title,
            "description": description,
            "categoryId": category,
            "tags": all_tags if all_tags else None,
        },
        "status": {
            "privacyStatus": privacy,
            "selfDeclaredMadeForKids": False,
        },
    }

    file_size = os.path.getsize(path)
    chunk_size = 8 * 1024 * 1024  # 8 MB
    media = MediaFileUpload(path, chunksize=chunk_size, resumable=True)
    request = youtube.videos().insert(
        part="snippet,status", body=body, media_body=media
    )

    response = None
    bytes_sent = 0
    while response is None:
        status, response = request.next_chunk()
        if status:
            bytes_sent = int(status.resumable_progress)
        else:
            bytes_sent = file_size
        if progress_callback:
            progress_callback(bytes_sent, file_size)

    return response["id"]


# ======================================================================
#  Playlist helper
# ======================================================================

def add_to_playlist(video_id: str, playlist_id: str):
    """Add a video to a YouTube playlist."""
    youtube = get_youtube()
    youtube.playlistItems().insert(
        part="snippet",
        body={
            "snippet": {
                "playlistId": playlist_id,
                "resourceId": {
                    "kind": "youtube#video",
                    "videoId": video_id,
                },
            }
        },
    ).execute()
    log.info(f"Added {video_id} to playlist {playlist_id}")


# ======================================================================
#  Thumbnail helper
# ======================================================================

def set_thumbnail(video_id: str, image_path: str):
    """Upload a custom thumbnail for a video."""
    youtube = get_youtube()
    youtube.thumbnails().set(
        videoId=video_id,
        media_body=MediaFileUpload(image_path),
    ).execute()
    log.info(f"Set thumbnail for {video_id}")


# ======================================================================
#  Caption / subtitle upload helper
# ======================================================================

SUBTITLE_EXTENSIONS = {".srt", ".vtt", ".sbv", ".sub", ".ass", ".ssa", ".ttml"}


def upload_captions(video_id: str, caption_path: str, language: str = "en", name: str = ""):
    """Upload a caption/subtitle track to a YouTube video."""
    youtube = get_youtube()
    youtube.captions().insert(
        part="snippet",
        body={
            "snippet": {
                "videoId": video_id,
                "language": language,
                "name": name or Path(caption_path).stem,
                "isDraft": False,
            }
        },
        media_body=MediaFileUpload(caption_path),
    ).execute()
    log.info(f"Uploaded captions ({language}) for {video_id}")


# ======================================================================
#  Core upload flow
# ======================================================================

async def _do_upload(item: QueueItem):
    """Download from Telegram, upload to YouTube with progress & retries."""
    event = item.event
    status = await event.reply("⬇️ Downloading…")
    tmpdir = tempfile.mkdtemp()

    try:
        # ---- Download with progress ----
        dl_tracker = ProgressTracker(status, "⬇️ Downloading…")
        total_size = 0
        if event.document:
            total_size = event.document.size or 0

        async def dl_progress(received, total):
            await dl_tracker.update(received, total or total_size)

        path = await event.download_media(
            file=tmpdir, progress_callback=dl_progress
        )
        await status.edit("⬆️ Uploading to YouTube…")

        # ---- Upload with retry ----
        video_id = None
        last_error = None
        loop = asyncio.get_running_loop()
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                ul_tracker = ProgressTracker(status, "⬆️ Uploading to YouTube…")

                def ul_progress(sent, total):
                    asyncio.run_coroutine_threadsafe(
                        ul_tracker.update(sent, total), loop
                    )

                video_id = await asyncio.to_thread(
                    upload_to_youtube_with_progress,
                    path,
                    item.title,
                    item.description,
                    item.tags,
                    item.category,
                    ul_progress,
                )
                break
            except Exception as e:
                last_error = e
                log.warning(f"Upload attempt {attempt}/{MAX_RETRIES} failed: {e}")
                if attempt < MAX_RETRIES:
                    await status.edit(
                        f"⚠️ Attempt {attempt} failed, retrying in {RETRY_DELAY}s…\n{e}"
                    )
                    await asyncio.sleep(RETRY_DELAY)

        if video_id is None:
            raise last_error  # type: ignore

        # ---- Playlist ----
        playlist_id = item.playlist_id or default_playlist
        playlist_note = ""
        if playlist_id:
            try:
                await asyncio.to_thread(add_to_playlist, video_id, playlist_id)
                playlist_note = f"\n📂 Added to playlist"
            except Exception as e:
                playlist_note = f"\n⚠️ Playlist failed: {e}"

        # ---- Store pending thumbnail state ----
        pending_thumbnail[event.chat_id] = video_id

        # ---- Save history ----
        save_history({
            "video_id": video_id,
            "title": item.title,
            "privacy": privacy,
            "uploaded_at": datetime.now().isoformat(),
        })

        url = f"https://youtu.be/{video_id}"
        await status.edit(
            f"✅ Done ({privacy})\n{url}{playlist_note}"
            f"\n\n💡 Reply with a photo to set thumbnail"
            f"\n💡 Reply with .srt/.vtt file to add captions"
        )
        log.info(f"Uploaded: {item.title} → {url}")

    except Exception as exc:
        await status.edit(f"❌ Failed after {MAX_RETRIES} attempts: {exc}")
        log.error(f"Upload failed: {exc}", exc_info=True)
    finally:
        # Cleanup temp files
        for name in os.listdir(tmpdir):
            os.remove(os.path.join(tmpdir, name))
        os.rmdir(tmpdir)


# ======================================================================
#  Event handlers
# ======================================================================

def is_video(e) -> bool:
    if e.video:
        return True
    doc = e.document
    return bool(doc and (doc.mime_type or "").startswith("video/"))


def is_photo(e) -> bool:
    return bool(e.photo)


def is_subtitle_file(e) -> bool:
    """Check if the message contains a subtitle file."""
    if not e.document:
        return False
    for attr in e.document.attributes:
        if isinstance(attr, DocumentAttributeFilename):
            ext = Path(attr.file_name).suffix.lower()
            return ext in SUBTITLE_EXTENSIONS
    return False


@client.on(events.NewMessage(from_users=OWNER_ID, func=is_video))
async def handle_video(event):
    title, description, tags = parse_caption(event.raw_text)
    item = QueueItem(
        event=event,
        title=title,
        description=description,
        tags=tags,
        category=default_category,
        playlist_id=default_playlist,
    )
    upload_queue.append(item)
    pos = len(upload_queue)
    if pos > 1:
        await event.reply(f"📋 Queued at position {pos}")
    asyncio.create_task(process_queue())


@client.on(events.NewMessage(from_users=OWNER_ID, func=is_photo))
async def handle_thumbnail(event):
    """Set thumbnail if there's a pending video."""
    video_id = pending_thumbnail.pop(event.chat_id, None)
    if not video_id:
        return
    status = await event.reply("🖼️ Setting thumbnail…")
    tmpdir = tempfile.mkdtemp()
    try:
        path = await event.download_media(file=tmpdir)
        await asyncio.to_thread(set_thumbnail, video_id, path)
        await status.edit(f"🖼️ Thumbnail set for https://youtu.be/{video_id}")
    except Exception as e:
        await status.edit(f"❌ Thumbnail failed: {e}")
    finally:
        for name in os.listdir(tmpdir):
            os.remove(os.path.join(tmpdir, name))
        os.rmdir(tmpdir)


@client.on(events.NewMessage(from_users=OWNER_ID, func=is_subtitle_file))
async def handle_subtitle(event):
    """Upload captions/subtitles to the most recent video."""
    video_id = pending_thumbnail.get(event.chat_id)
    if not video_id:
        await event.reply("❌ No recent video to attach captions to.")
        return

    # Parse language from caption, e.g. "en" or "bn"
    lang = (event.raw_text or "").strip() or "en"

    status = await event.reply(f"📝 Uploading captions ({lang})…")
    tmpdir = tempfile.mkdtemp()
    try:
        path = await event.download_media(file=tmpdir)
        await asyncio.to_thread(upload_captions, video_id, path, lang)
        await status.edit(f"📝 Captions ({lang}) added to https://youtu.be/{video_id}")
    except Exception as e:
        await status.edit(f"❌ Caption upload failed: {e}")
    finally:
        for name in os.listdir(tmpdir):
            os.remove(os.path.join(tmpdir, name))
        os.rmdir(tmpdir)


# ---- Commands ----

@client.on(events.NewMessage(from_users=OWNER_ID, pattern=r"/start"))
async def cmd_start(event):
    await event.reply(
        "🎬 **YouTube Upload Bot**\n\n"
        "**Send a video** to upload it. Caption format:\n"
        "`Title goes here`\n"
        "`Description on next lines`\n"
        "`#tag1 #tag2`\n\n"
        "**Commands:**\n"
        "/privacy `private|unlisted|public`\n"
        "/tags `tag1, tag2, …` — set default tags\n"
        "/category `ID` — set default category\n"
        "/playlist `PLAYLIST_ID` — auto-add to playlist\n"
        "/playlist off — disable auto-playlist\n"
        "/history — recent uploads\n"
        "/queue — show queue status\n"
        "/help — show this message"
    )


@client.on(events.NewMessage(from_users=OWNER_ID, pattern=r"/help"))
async def cmd_help(event):
    await cmd_start(event)


@client.on(events.NewMessage(from_users=OWNER_ID, pattern=r"/privacy\s*(.*)"))
async def cmd_privacy(event):
    global privacy
    arg = event.pattern_match.group(1).strip().lower()
    if arg not in ("private", "unlisted", "public"):
        await event.reply(
            f"Current: **{privacy}**\n"
            "Usage: `/privacy private|unlisted|public`"
        )
        return
    privacy = arg
    await event.reply(f"✅ Privacy set to **{privacy}**")
    log.info(f"Privacy changed to {privacy}")


@client.on(events.NewMessage(from_users=OWNER_ID, pattern=r"/tags\s*(.*)"))
async def cmd_tags(event):
    global default_tags
    arg = event.pattern_match.group(1).strip()
    if not arg:
        current = ", ".join(default_tags) if default_tags else "none"
        await event.reply(f"Current default tags: {current}\nUsage: `/tags tag1, tag2`")
        return
    default_tags = [t.strip().lstrip("#") for t in arg.split(",") if t.strip()]
    await event.reply(f"✅ Default tags: {', '.join(default_tags)}")


@client.on(events.NewMessage(from_users=OWNER_ID, pattern=r"/category\s*(.*)"))
async def cmd_category(event):
    global default_category
    arg = event.pattern_match.group(1).strip()
    if not arg or arg not in CATEGORY_MAP:
        lines = "\n".join(f"`{k}` — {v}" for k, v in sorted(CATEGORY_MAP.items(), key=lambda x: int(x[0])))
        await event.reply(
            f"Current: **{default_category}** ({CATEGORY_MAP.get(default_category, '?')})\n\n"
            f"**Categories:**\n{lines}\n\nUsage: `/category 22`"
        )
        return
    default_category = arg
    await event.reply(f"✅ Category: **{arg}** ({CATEGORY_MAP[arg]})")


@client.on(events.NewMessage(from_users=OWNER_ID, pattern=r"/playlist\s*(.*)"))
async def cmd_playlist(event):
    global default_playlist
    arg = event.pattern_match.group(1).strip()
    if not arg:
        current = default_playlist or "none"
        await event.reply(
            f"Current playlist: `{current}`\n"
            "Usage: `/playlist PLAYLIST_ID` or `/playlist off`"
        )
        return
    if arg.lower() == "off":
        default_playlist = None
        await event.reply("✅ Auto-playlist disabled")
    else:
        default_playlist = arg
        await event.reply(f"✅ Auto-playlist: `{arg}`")


@client.on(events.NewMessage(from_users=OWNER_ID, pattern=r"/history"))
async def cmd_history(event):
    if not Path(HISTORY_FILE).exists():
        await event.reply("No uploads yet.")
        return
    try:
        history = json.loads(Path(HISTORY_FILE).read_text())
    except (json.JSONDecodeError, OSError):
        await event.reply("No uploads yet.")
        return

    recent = history[-10:]  # last 10
    lines = []
    for i, h in enumerate(reversed(recent), 1):
        title = h.get("title", "?")
        vid = h.get("video_id", "?")
        ts = h.get("uploaded_at", "?")[:16]
        priv = h.get("privacy", "?")
        lines.append(f"{i}. [{title}](https://youtu.be/{vid}) ({priv}) — {ts}")

    await event.reply("📜 **Recent Uploads:**\n\n" + "\n".join(lines))


@client.on(events.NewMessage(from_users=OWNER_ID, pattern=r"/queue"))
async def cmd_queue(event):
    if not upload_queue:
        await event.reply("📋 Queue is empty.")
    else:
        lines = [f"{i+1}. {item.title}" for i, item in enumerate(upload_queue)]
        await event.reply("📋 **Upload Queue:**\n\n" + "\n".join(lines))


# ======================================================================
#  Main
# ======================================================================

if __name__ == "__main__":
    log.info("Starting YouTube Upload Bot…")
    client.start(bot_token=BOT_TOKEN)
    log.info("Bot running.")
    client.run_until_disconnected()
