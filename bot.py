import asyncio
import logging
import os
import re
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from telethon import TelegramClient, events, Button
from telethon.tl.types import DocumentAttributeFilename

import state
from state import (
    API_ID,
    API_HASH,
    BOT_TOKEN,
    OWNER_ID,
    SESSION_FILE,
    MAX_DOWNLOAD_GB,
    DOWNLOAD_DIR,
    CATEGORY_MAP,
    JobState,
    create_job,
    get_job,
    get_waiting_title_job,
)
from downloader import (
    clean_title,
    extract_url,
    is_host_allowed,
    inspect_url,
    check_disk_space,
    format_size,
    format_time,
    format_speed,
    download_url_stream,
    download_tg_video,
)
from youtube import (
    upload_to_youtube_with_progress,
    save_history,
    add_to_playlist,
    set_thumbnail,
    upload_captions,
)

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("ytbot")

# Initialize Telethon client
client = TelegramClient(SESSION_FILE, API_ID, API_HASH)


# ======================================================================
#  Card buttons & text helpers
# ======================================================================

def make_preview_buttons(job_id: str):
    return [
        [
            Button.inline("⬇️ Download", data=f"dl:{job_id}"),
            Button.inline("❌ Cancel", data=f"cancel:{job_id}"),
        ]
    ]


def make_ready_buttons(job_id: str, privacy: str):
    return [
        [
            Button.inline("✏️ Title", data=f"title:{job_id}"),
            Button.inline(f"🔒 {privacy.title()}", data=f"priv:{job_id}"),
        ],
        [
            Button.inline("🚀 Upload", data=f"up:{job_id}"),
            Button.inline("❌ Cancel", data=f"cancel:{job_id}"),
        ],
    ]


def make_cancel_button(job_id: str):
    return [[Button.inline("❌ Cancel", data=f"cancel:{job_id}")]]


def render_ready_card_text(job: JobState) -> str:
    text = (
        f"📹 **Ready to Upload**\n\n"
        f"**Title:** {job.title}\n"
        f"**Size:** {format_size(job.file_size)}\n"
        f"**Privacy:** {job.privacy}\n\n"
        f"*(Note: unaudited API projects stay private)*"
    )
    if job.waiting_for_title:
        text += "\n\n✏️ *Send the new title as a message (max 100 characters).*"
    return text


# ======================================================================
#  Throttled progress tracker (at most once every 3 seconds)
# ======================================================================

class ProgressTracker:
    def __init__(self, card_msg, job_id: str, prefix: str, min_interval: float = 3.0):
        self.card_msg = card_msg
        self.job_id = job_id
        self.prefix = prefix
        self.min_interval = min_interval
        self._last_update = 0.0
        self._last_text = ""
        self._start_time: Optional[float] = None
        self._samples: list[tuple[float, int]] = []

    async def update(self, current: int, total: int, force: bool = False):
        now = time.time()
        if self._start_time is None:
            self._start_time = now

        self._samples.append((now, current))
        cutoff = now - 10.0
        self._samples = [(t, b) for t, b in self._samples if t >= cutoff]

        if not force and (now - self._last_update < self.min_interval):
            return

        elapsed = now - self._start_time
        if len(self._samples) >= 2:
            dt = self._samples[-1][0] - self._samples[0][0]
            db = self._samples[-1][1] - self._samples[0][1]
            speed = db / dt if dt > 0 else 0
        else:
            speed = current / elapsed if elapsed > 0 else 0

        if total > 0:
            pct = min(current / total * 100, 100)
            bar_len = 14
            filled = int(bar_len * pct / 100)
            bar = "█" * filled + "░" * (bar_len - filled)
            mb_done = current / (1024 * 1024)
            mb_total = total / (1024 * 1024)
            rem = max(0, total - current)
            eta = rem / speed if speed > 0 else -1

            text = (
                f"{self.prefix}\n\n"
                f"{bar} {pct:.0f}%\n"
                f"{mb_done:.1f} / {mb_total:.1f} MB\n"
                f"Elapsed: {format_time(elapsed)}\n"
                f"ETA: {format_time(eta)}\n"
                f"Speed: {format_speed(speed)}"
            )
        else:
            mb_done = current / (1024 * 1024)
            text = (
                f"{self.prefix}\n\n"
                f"{mb_done:.1f} MB transferred\n"
                f"Elapsed: {format_time(elapsed)}\n"
                f"Speed: {format_speed(speed)}"
            )

        if text == self._last_text:
            return

        try:
            buttons = make_cancel_button(self.job_id)
            await self.card_msg.edit(text, buttons=buttons)
            self._last_text = text
            self._last_update = now
        except Exception:
            pass


# ======================================================================
#  Background tasks: Download & Upload
# ======================================================================

async def start_download_task(job: JobState):
    """Download the media stream and display progress on the card."""
    state.active_job_id = job.id
    job.status = "downloading"
    tracker = ProgressTracker(job.card_msg, job.id, "⬇️ **Downloading…**")
    await tracker.update(0, job.file_size, force=True)

    tmp_dir = tempfile.mkdtemp(dir=DOWNLOAD_DIR)
    job.tmp_dir = tmp_dir
    dest_path = os.path.join(tmp_dir, job.filename or "video.mp4")

    async def dl_progress(received, total):
        await tracker.update(received, total or job.file_size)

    try:
        if job.source_type == "url":
            path = await download_url_stream(
                job.url,
                dest_path,
                job.file_size,
                dl_progress,
                job.cancel_event,
            )
        else:
            path = await download_tg_video(
                client,
                job.tg_event,
                dest_path,
                job.file_size,
                dl_progress,
                job.cancel_event,
            )

        job.local_path = path
        job.status = "downloaded"
        if os.path.exists(path):
            job.file_size = os.path.getsize(path)

        ready_text = render_ready_card_text(job)
        ready_buttons = make_ready_buttons(job.id, job.privacy)
        await job.card_msg.edit(ready_text, buttons=ready_buttons)

    except asyncio.CancelledError:
        log.info(f"Download cancelled for job {job.id}")
        job.cleanup()
        if state.active_job_id == job.id:
            state.active_job_id = None
        await job.card_msg.edit(f"❌ Cancelled: **{job.title}**", buttons=None)
    except Exception as exc:
        log.error(f"Download failed for job {job.id}: {exc}", exc_info=True)
        job.cleanup()
        if state.active_job_id == job.id:
            state.active_job_id = None
        await job.card_msg.edit(f"❌ Download failed: {exc}", buttons=None)


async def start_upload_task(job: JobState):
    """Upload downloaded video to YouTube using resumable chunks."""
    state.active_job_id = job.id
    job.status = "uploading"
    tracker = ProgressTracker(job.card_msg, job.id, "⬆️ **Uploading to YouTube…**")
    await tracker.update(0, job.file_size, force=True)

    loop = asyncio.get_running_loop()

    def ul_progress(sent, total):
        asyncio.run_coroutine_threadsafe(tracker.update(sent, total), loop)

    def cancel_check():
        return job.cancel_event.is_set()

    try:
        video_id = await asyncio.to_thread(
            upload_to_youtube_with_progress,
            job.local_path,
            job.title,
            job.description,
            job.tags,
            job.category,
            job.privacy,
            ul_progress,
            cancel_check,
        )

        playlist_note = ""
        playlist_id = job.playlist_id or state.default_playlist
        if playlist_id:
            try:
                await asyncio.to_thread(add_to_playlist, video_id, playlist_id)
                playlist_note = "\n📂 Added to playlist"
            except Exception as e:
                playlist_note = f"\n⚠️ Playlist failed: {e}"

        save_history({
            "video_id": video_id,
            "title": job.title,
            "privacy": job.privacy,
            "uploaded_at": datetime.now().isoformat(),
        })

        state.pending_thumbnail[job.chat_id] = video_id
        url = f"https://youtu.be/{video_id}"
        await job.card_msg.edit(
            f"✅ **Uploaded successfully!**\n\n"
            f"🔗 {url}\n"
            f"**Title:** {job.title}\n"
            f"**Privacy:** {job.privacy}{playlist_note}\n\n"
            f"💡 Reply with a photo to set thumbnail\n"
            f"💡 Reply with .srt/.vtt file to add captions",
            buttons=None,
        )
        job.status = "done"

    except asyncio.CancelledError:
        log.info(f"Upload cancelled for job {job.id}")
        await job.card_msg.edit(f"❌ Cancelled: **{job.title}**", buttons=None)
    except Exception as exc:
        log.error(f"Upload failed for job {job.id}: {exc}", exc_info=True)
        await job.card_msg.edit(f"❌ Upload failed: {exc}", buttons=None)
    finally:
        job.cleanup()
        if state.active_job_id == job.id:
            state.active_job_id = None


# ======================================================================
#  Caption parser
# ======================================================================

def parse_caption(raw_text: str) -> tuple[str, str, list[str]]:
    text = (raw_text or "").strip()
    if not text:
        return "", "", []

    lines = text.split("\n")
    title = lines[0].strip()
    rest = "\n".join(lines[1:]).strip()

    tag_pattern = re.compile(r"#(\w+)")
    inline_tags = tag_pattern.findall(rest)
    desc_lines = [l for l in rest.split("\n") if not l.strip().startswith("#")]
    description = "\n".join(desc_lines).strip()

    return title[:100], description, inline_tags


# ======================================================================
#  Event Handlers: Callbacks & Messages
# ======================================================================

@client.on(events.CallbackQuery)
async def handle_callback(event):
    if event.sender_id != OWNER_ID:
        return

    data = event.data.decode("utf-8") if isinstance(event.data, bytes) else str(event.data)
    if ":" not in data:
        return

    action, job_id = data.split(":", 1)
    job = get_job(job_id)
    if not job:
        await event.answer("⚠️ Job not found or expired.", alert=True)
        return

    if action == "cancel":
        job.cancel_event.set()
        job.cleanup()
        if state.active_job_id == job.id:
            state.active_job_id = None
        job.status = "cancelled"
        try:
            await event.edit(f"❌ Cancelled: **{job.title}**", buttons=None)
        except Exception:
            pass
        await event.answer("Cancelled.")
        return

    elif action == "dl":
        if state.active_job_id is not None and state.active_job_id != job.id:
            await event.answer("⚠️ Another job is currently active. Finish or cancel it first.", alert=True)
            return
        await event.answer("Starting download…")
        asyncio.create_task(start_download_task(job))

    elif action == "title":
        job.waiting_for_title = True
        await event.answer("Send the new title in your next message.")
        ready_text = render_ready_card_text(job)
        ready_buttons = make_ready_buttons(job.id, job.privacy)
        await event.edit(ready_text, buttons=ready_buttons)

    elif action == "priv":
        cycle = ["private", "unlisted", "public"]
        cur_idx = cycle.index(job.privacy) if job.privacy in cycle else 0
        job.privacy = cycle[(cur_idx + 1) % len(cycle)]
        await event.answer(f"Privacy set to {job.privacy}")
        ready_text = render_ready_card_text(job)
        ready_buttons = make_ready_buttons(job.id, job.privacy)
        await event.edit(ready_text, buttons=ready_buttons)

    elif action == "up":
        if state.active_job_id is not None and state.active_job_id != job.id:
            await event.answer("⚠️ Another job is currently active. Finish or cancel it first.", alert=True)
            return
        if not job.local_path or not os.path.exists(job.local_path):
            await event.answer("⚠️ File not found locally. Please re-download.", alert=True)
            return
        await event.answer("Starting upload…")
        asyncio.create_task(start_upload_task(job))


@client.on(events.NewMessage(from_users=OWNER_ID))
async def handle_message(event):
    if event.sender_id != OWNER_ID:
        return

    # Check if user is replying with a new title for an active job
    waiting_job = get_waiting_title_job(event.chat_id)
    if waiting_job and event.raw_text:
        new_title = event.raw_text.strip()[:100]
        if new_title:
            waiting_job.title = new_title
            waiting_job.waiting_for_title = False
            ready_text = render_ready_card_text(waiting_job)
            ready_buttons = make_ready_buttons(waiting_job.id, waiting_job.privacy)
            if waiting_job.card_msg:
                try:
                    await waiting_job.card_msg.edit(ready_text, buttons=ready_buttons)
                except Exception:
                    pass
            await event.reply(f"✅ Title updated to:\n`{new_title}`")
            return

    text = (event.raw_text or "").strip()
    if text.startswith("/"):
        return

    # Check for direct URL
    url = extract_url(text)
    if url:
        allowed, err = is_host_allowed(url)
        if not allowed:
            await event.reply(f"❌ {err}")
            return

        status_msg = await event.reply("🔍 Inspecting URL…")
        try:
            final_url, filename, size = await inspect_url(url)
            title = clean_title(filename)
            job = create_job(
                chat_id=event.chat_id,
                source_type="url",
                title=title,
                file_size=size,
                filename=filename,
                url=final_url,
            )
            job.card_msg = status_msg

            preview_text = (
                f"📹 **Video Found**\n\n"
                f"**Title:** {job.title}\n"
                f"**Size:** {format_size(job.file_size)}"
            )
            buttons = make_preview_buttons(job.id)
            await status_msg.edit(preview_text, buttons=buttons)
        except Exception as exc:
            await status_msg.edit(f"❌ Error: {exc}")
        return


def is_video(e) -> bool:
    if e.video:
        return True
    doc = e.document
    return bool(doc and (doc.mime_type or "").startswith("video/"))


@client.on(events.NewMessage(from_users=OWNER_ID, func=is_video))
async def handle_video(event):
    if event.sender_id != OWNER_ID:
        return

    total_size = 0
    if event.document:
        total_size = event.document.size or 0
    elif event.video:
        total_size = event.video.size or 0

    max_bytes = int(MAX_DOWNLOAD_GB * 1024 * 1024 * 1024)
    if total_size > max_bytes:
        await event.reply(
            f"❌ File size ({format_size(total_size)}) exceeds MAX_DOWNLOAD_GB ({MAX_DOWNLOAD_GB} GB)"
        )
        return

    try:
        check_disk_space(total_size)
    except OSError as e:
        await event.reply(f"❌ {e}")
        return

    filename = "video.mp4"
    if event.document and event.document.attributes:
        for attr in event.document.attributes:
            if isinstance(attr, DocumentAttributeFilename):
                filename = attr.file_name
                break

    caption_title, desc, tags = parse_caption(event.raw_text)
    title = caption_title if caption_title else clean_title(filename)

    status_msg = await event.reply("📹 **Video Received**…")
    job = create_job(
        chat_id=event.chat_id,
        source_type="telegram",
        title=title,
        file_size=total_size,
        filename=filename,
        tg_event=event,
        description=desc,
        tags=tags,
    )
    job.card_msg = status_msg

    preview_text = (
        f"📹 **Video Received**\n\n"
        f"**Title:** {job.title}\n"
        f"**Size:** {format_size(job.file_size)}"
    )
    buttons = make_preview_buttons(job.id)
    await status_msg.edit(preview_text, buttons=buttons)


@client.on(events.NewMessage(from_users=OWNER_ID, func=lambda e: bool(e.photo)))
async def handle_thumbnail(event):
    video_id = state.pending_thumbnail.pop(event.chat_id, None)
    if not video_id:
        return
    status = await event.reply("🖼️ Setting thumbnail…")
    tmpdir = tempfile.mkdtemp(dir=DOWNLOAD_DIR)
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


SUBTITLE_EXTENSIONS = {".srt", ".vtt", ".sbv", ".sub", ".ass", ".ssa", ".ttml"}


def is_subtitle_file(e) -> bool:
    if not e.document:
        return False
    for attr in e.document.attributes:
        if isinstance(attr, DocumentAttributeFilename):
            ext = Path(attr.file_name).suffix.lower()
            return ext in SUBTITLE_EXTENSIONS
    return False


@client.on(events.NewMessage(from_users=OWNER_ID, func=is_subtitle_file))
async def handle_subtitle(event):
    video_id = state.pending_thumbnail.get(event.chat_id)
    if not video_id:
        await event.reply("❌ No recent video to attach captions to.")
        return

    lang = (event.raw_text or "").strip() or "en"
    status = await event.reply(f"📝 Uploading captions ({lang})…")
    tmpdir = tempfile.mkdtemp(dir=DOWNLOAD_DIR)
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


# ======================================================================
#  Commands
# ======================================================================

@client.on(events.NewMessage(from_users=OWNER_ID, pattern=r"/start"))
async def cmd_start(event):
    await event.reply(
        "🎬 **YouTube Upload Bot**\n\n"
        "• **Send a direct link** (HTTPS from allowed domain) to inspect and upload.\n"
        "• **Send a video file** directly to inspect and upload.\n\n"
        "**Commands:**\n"
        "/privacy `private|unlisted|public`\n"
        "/tags `tag1, tag2, …` — set default tags\n"
        "/category `ID` — set default category\n"
        "/playlist `PLAYLIST_ID` — auto-add to playlist\n"
        "/playlist off — disable auto-playlist\n"
        "/history — recent uploads\n"
        "/queue — current active job status\n"
        "/help — show this message"
    )


@client.on(events.NewMessage(from_users=OWNER_ID, pattern=r"/help"))
async def cmd_help(event):
    await cmd_start(event)


@client.on(events.NewMessage(from_users=OWNER_ID, pattern=r"/privacy\s*(.*)"))
async def cmd_privacy(event):
    arg = event.pattern_match.group(1).strip().lower()
    if arg not in ("private", "unlisted", "public"):
        await event.reply(
            f"Current default: **{state.PRIVACY}**\n"
            "Usage: `/privacy private|unlisted|public`"
        )
        return
    state.PRIVACY = arg
    await event.reply(f"✅ Default privacy set to **{state.PRIVACY}**")


@client.on(events.NewMessage(from_users=OWNER_ID, pattern=r"/tags\s*(.*)"))
async def cmd_tags(event):
    arg = event.pattern_match.group(1).strip()
    if not arg:
        current = ", ".join(state.default_tags) if state.default_tags else "none"
        await event.reply(f"Current default tags: {current}\nUsage: `/tags tag1, tag2`")
        return
    state.default_tags = [t.strip().lstrip("#") for t in arg.split(",") if t.strip()]
    await event.reply(f"✅ Default tags: {', '.join(state.default_tags)}")


@client.on(events.NewMessage(from_users=OWNER_ID, pattern=r"/category\s*(.*)"))
async def cmd_category(event):
    arg = event.pattern_match.group(1).strip()
    if not arg or arg not in CATEGORY_MAP:
        lines = "\n".join(f"`{k}` — {v}" for k, v in sorted(CATEGORY_MAP.items(), key=lambda x: int(x[0])))
        await event.reply(
            f"Current: **{state.default_category}** ({CATEGORY_MAP.get(state.default_category, '?')})\n\n"
            f"**Categories:**\n{lines}\n\nUsage: `/category 22`"
        )
        return
    state.default_category = arg
    await event.reply(f"✅ Category: **{arg}** ({CATEGORY_MAP[arg]})")


@client.on(events.NewMessage(from_users=OWNER_ID, pattern=r"/playlist\s*(.*)"))
async def cmd_playlist(event):
    arg = event.pattern_match.group(1).strip()
    if not arg:
        current = state.default_playlist or "none"
        await event.reply(
            f"Current playlist: `{current}`\n"
            "Usage: `/playlist PLAYLIST_ID` or `/playlist off`"
        )
        return
    if arg.lower() == "off":
        state.default_playlist = None
        await event.reply("✅ Auto-playlist disabled")
    else:
        state.default_playlist = arg
        await event.reply(f"✅ Auto-playlist: `{arg}`")


@client.on(events.NewMessage(from_users=OWNER_ID, pattern=r"/history"))
async def cmd_history(event):
    hist_path = Path(state.HISTORY_FILE)
    if not hist_path.exists():
        await event.reply("No uploads yet.")
        return
    try:
        import json
        history = json.loads(hist_path.read_text())
    except (json.JSONDecodeError, OSError):
        await event.reply("No uploads yet.")
        return

    recent = history[-10:]
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
    if state.active_job_id:
        active = get_job(state.active_job_id)
        title = active.title if active else state.active_job_id
        status = active.status if active else "running"
        await event.reply(f"📋 **Active job:** {title} ({status})")
    else:
        await event.reply("📋 No active job.")


# ======================================================================
#  Main entrypoint
# ======================================================================

if __name__ == "__main__":
    log.info("Starting YouTube Upload Bot…")
    client.start(bot_token=BOT_TOKEN)
    log.info("Bot running.")
    client.run_until_disconnected()
