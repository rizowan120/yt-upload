import json
import logging
import os
from pathlib import Path
from typing import Optional, Callable

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from state import TOKEN_FILE, SCOPES, HISTORY_FILE, default_tags

log = logging.getLogger("ytbot")


def get_credentials() -> Credentials:
    """Load and refresh YouTube OAuth credentials without printing or logging them."""
    token_path = Path(TOKEN_FILE)
    if not token_path.exists():
        raise FileNotFoundError(
            f"{TOKEN_FILE} not found. Run auth.py or auth_manual.py first."
        )
    creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)
    if not creds.valid:
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            token_path.write_text(creds.to_json())
            log.info("Refreshed YouTube token")
        else:
            raise RuntimeError("YouTube token is invalid and cannot be refreshed.")
    return creds


def get_youtube():
    """Build authenticated YouTube API service."""
    return build("youtube", "v3", credentials=get_credentials())


def save_history(entry: dict):
    """Append upload entry to history file."""
    history = []
    hist_path = Path(HISTORY_FILE)
    if hist_path.exists():
        try:
            history = json.loads(hist_path.read_text())
        except (json.JSONDecodeError, OSError):
            pass
    history.append(entry)
    hist_path.write_text(json.dumps(history, indent=2))


def upload_to_youtube_with_progress(
    path: str,
    title: str,
    description: str,
    tags: list[str],
    category: str,
    privacy: str,
    progress_callback: Optional[Callable[[int, int], None]] = None,
    cancel_check: Optional[Callable[[], bool]] = None,
) -> str:
    """Upload video to YouTube using resumable chunks and progress reporting."""
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
    chunk_size = 100 * 1024 * 1024  # 100 MB chunks
    media = MediaFileUpload(path, chunksize=chunk_size, resumable=True)
    request = youtube.videos().insert(
        part="snippet,status", body=body, media_body=media
    )

    response = None
    bytes_sent = 0
    while response is None:
        if cancel_check and cancel_check():
            raise RuntimeError("Upload cancelled by user")

        status, response = request.next_chunk()
        if status:
            bytes_sent = int(status.resumable_progress)
        else:
            bytes_sent = file_size
        if progress_callback:
            progress_callback(bytes_sent, file_size)

    return response["id"]


def add_to_playlist(video_id: str, playlist_id: str):
    """Add video to YouTube playlist."""
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


def set_thumbnail(video_id: str, image_path: str):
    """Upload custom thumbnail for a video."""
    youtube = get_youtube()
    youtube.thumbnails().set(
        videoId=video_id,
        media_body=MediaFileUpload(image_path),
    ).execute()
    log.info(f"Set thumbnail for {video_id}")


def upload_captions(video_id: str, caption_path: str, language: str = "en", name: str = ""):
    """Upload caption/subtitle track."""
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
