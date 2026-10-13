import os
import shutil
import tempfile
import time
import asyncio
from dataclasses import dataclass, field
from typing import Optional, Dict
from dotenv import load_dotenv

# Load .env without printing or displaying its contents
load_dotenv()

# Environment Configuration
TOKEN_FILE = os.environ.get("TOKEN_FILE", "token.json")
SESSION_FILE = os.environ.get("SESSION_FILE", "ytbot")
ALLOWED_HOSTS = [
    h.strip().lower()
    for h in os.environ.get("ALLOWED_HOSTS", "drive.jonallen.workers.dev").split(",")
    if h.strip()
]
MAX_DOWNLOAD_GB = float(os.environ.get("MAX_DOWNLOAD_GB", "5"))
DOWNLOAD_DIR = os.environ.get("DOWNLOAD_DIR") or None
PRIVACY = os.environ.get("PRIVACY", "private").lower()
HISTORY_FILE = os.environ.get("HISTORY_FILE", "upload_history.json")

# Bot identity / Telegram config
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
OWNER_ID = int(os.environ["OWNER_ID"])

# YouTube Defaults & Constants
SCOPES = [
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/youtube.force-ssl",
]
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

default_tags: list[str] = []
default_category: str = "22"
default_playlist: Optional[str] = None
pending_thumbnail: dict = {}


@dataclass
class JobState:
    id: str
    chat_id: int
    source_type: str  # "url" or "telegram"
    title: str = ""
    file_size: int = 0
    filename: str = ""
    privacy: str = PRIVACY
    url: Optional[str] = None
    tg_event: Optional[object] = None
    card_msg: Optional[object] = None
    local_path: Optional[str] = None
    tmp_dir: Optional[str] = None
    status: str = "preview"  # preview | downloading | downloaded | uploading | done | cancelled | error
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)
    waiting_for_title: bool = False
    description: str = ""
    tags: list[str] = field(default_factory=list)
    category: str = "22"
    playlist_id: Optional[str] = None
    created_at: float = field(default_factory=time.time)

    def cleanup(self):
        """Clean up local temporary directory and files."""
        if self.tmp_dir and os.path.exists(self.tmp_dir):
            try:
                shutil.rmtree(self.tmp_dir, ignore_errors=True)
            except Exception:
                pass
        self.local_path = None
        self.tmp_dir = None


# Registry of jobs: callback job_id -> JobState
jobs: Dict[str, JobState] = {}

# Active lock and job tracker: strictly one download/upload job at a time
job_lock = asyncio.Lock()
active_job_id: Optional[str] = None

_job_counter = 0


def create_job(
    chat_id: int,
    source_type: str,
    title: str,
    file_size: int,
    filename: str,
    url: Optional[str] = None,
    tg_event: Optional[object] = None,
    description: str = "",
    tags: Optional[list[str]] = None,
    category: str = "22",
    playlist_id: Optional[str] = None,
) -> JobState:
    global _job_counter
    _job_counter += 1
    job_id = f"j{_job_counter}"
    job = JobState(
        id=job_id,
        chat_id=chat_id,
        source_type=source_type,
        title=title,
        file_size=file_size,
        filename=filename,
        privacy=PRIVACY,
        url=url,
        tg_event=tg_event,
        description=description,
        tags=tags or [],
        category=category,
        playlist_id=playlist_id,
    )
    jobs[job_id] = job
    return job


def get_job(job_id: str) -> Optional[JobState]:
    return jobs.get(job_id)


def get_waiting_title_job(chat_id: int) -> Optional[JobState]:
    """Find a job waiting for title input from this chat."""
    for job in jobs.values():
        if job.chat_id == chat_id and job.waiting_for_title:
            return job
    return None
