#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import hashlib
import html
import json
import math
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional, TypeVar
from urllib.parse import quote

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from telethon import TelegramClient, helpers
from telethon.errors import FloodWaitError, RPCError, ServerError
from telethon.network import MTProtoSender
from telethon.sessions import StringSession
from telethon.tl.alltlobjects import LAYER
from telethon.tl.functions import InitConnectionRequest, InvokeWithLayerRequest
from telethon.tl.functions.channels import GetChannelsRequest
from telethon.tl.functions.help import GetConfigRequest
from telethon.tl.functions.upload import SaveBigFilePartRequest, SaveFilePartRequest
from telethon.tl.types import InputChannel, InputFile, InputFileBig
from telethon.utils import get_peer_id

T = TypeVar("T")


def log(msg: str) -> None:
    print(msg, flush=True)


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        sys.exit(f"Missing required environment variable: {name}")
    return value


# ============================================================================
# CONFIGURATION
# ============================================================================
GH_USERNAME = require_env("GH_USERNAME")
GH_TOKEN = os.environ.get("GH_TOKEN", "").strip()
TG_API_HASH = require_env("TG_API_HASH")
TG_BOT_TOKEN = require_env("TG_BOT_TOKEN")
try:
    TG_API_ID = int(require_env("TG_API_ID"))
except ValueError:
    sys.exit("TG_API_ID must be an integer")

_raw_group = require_env("TG_GROUP_ID")
TG_GROUP_ID: int | str = int(_raw_group) if re.fullmatch(r"-?\d+", _raw_group) else _raw_group

DB_FILE = "database.json"
WORK_DIR = "temp_downloads"

PART_SIZE = 512 * 1024
BIG_FILE_THRESHOLD = 10 * 1024 * 1024
MAX_UPLOAD_BYTES = 1950 * 1024 * 1024  # 3900 parts, under Telegram's 4000-part cap
UPLOAD_SOCKETS = max(1, min(8, int(os.environ.get("TG_UPLOAD_SOCKETS", "4"))))
INFLIGHT_PER_SOCKET = 4
PART_RETRIES = 6
PART_TIMEOUT_S = 90
HTTP_TIMEOUT = (15, 60)
DOWNLOAD_ATTEMPTS = 3
DOWNLOAD_TIMEOUT_S = 4 * 3600
GIT_TIMEOUT_S = 3600
TITLE_MAX = 200
CHANGELOG_MAX = 3000
TOPIC_TITLE_MAX = 120
STRICT_EXIT = os.environ.get("STRICT_EXIT") == "1"

GIT_ENV = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}
TRANSIENT = (OSError, asyncio.TimeoutError, ServerError)


# ============================================================================
# GITHUB API
# ============================================================================
def build_gh_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "stars-archiver",
    })
    if GH_TOKEN:
        s.headers["Authorization"] = f"Bearer {GH_TOKEN}"
    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


GH = build_gh_session()


def gh_get_json(url: str) -> Any:
    for _ in range(5):
        r = GH.get(url, timeout=HTTP_TIMEOUT)
        limited = r.status_code in (403, 429) and (
            r.headers.get("X-RateLimit-Remaining") == "0" or "Retry-After" in r.headers
        )
        if limited:
            if "Retry-After" in r.headers:
                wait = int(r.headers["Retry-After"])
            else:
                wait = int(r.headers.get("X-RateLimit-Reset", "0")) - int(time.time())
            wait = min(max(wait, 1) + 2, 3700)
            log(f"GitHub rate limit hit. Sleeping {wait}s...")
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"GitHub rate limit unrecoverable: {url}")


def gh_paginate(url: str) -> list[dict]:
    items: list[dict] = []
    sep = "&" if "?" in url else "?"
    page = 1
    while True:
        data = gh_get_json(f"{url}{sep}per_page=100&page={page}")
        if not data:
            break
        items.extend(data)
        if len(data) < 100:
            break
        page += 1
    return items


def get_starred_repos(username: str) -> list[dict]:
    repos = gh_paginate(f"https://api.github.com/users/{quote(username)}/starred")
    return list(reversed(repos))


def get_repo_releases(full_name: str) -> list[dict]:
    try:
        releases = gh_paginate(f"https://api.github.com/repos/{full_name}/releases")
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code in (404, 451):
            return []
        raise
    return list(reversed(releases))


# ============================================================================
# LOCAL HELPERS
# ============================================================================
def load_db() -> dict:
    if not os.path.exists(DB_FILE):
        return {"db_topic_id": None, "projects": {}}
    try:
        with open(DB_FILE, "r", encoding="utf-8") as f:
            db = json.load(f)
        if not isinstance(db, dict):
            raise ValueError("root must be an object")
    except (json.JSONDecodeError, ValueError) as e:
        sys.exit(f"{DB_FILE} is corrupt ({e}); refusing to continue to avoid duplicate uploads")
    db.setdefault("db_topic_id", None)
    db.setdefault("projects", {})
    return db


def save_db(db: dict) -> None:
    tmp = f"{DB_FILE}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(db, f, indent=2, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, DB_FILE)


def format_size(size_bytes: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} TB"


def sha256_hex(file_path: str) -> str:
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(8 * 1024 * 1024):
            h.update(chunk)
    return h.hexdigest()


def get_file_icon(filename: str) -> str:
    name = filename.lower()
    if name.endswith(".apk"):
        return "🤖"
    if name.endswith((".exe", ".msi")):
        return "🪟"
    if name.endswith((".dmg", ".pkg")):
        return "🍎"
    if name.endswith((".appimage", ".deb", ".rpm", ".flatpak", ".tar.gz")):
        return "🐧"
    if name.endswith(".bundle"):
        return "🧬"
    return "💿"


def safe_filename(name: str) -> str:
    base = os.path.basename(name.replace("\\", "/"))
    base = re.sub(r'[\x00-\x1f/\\:*?"<>|]', "_", base).strip().strip(".")
    stem, ext = os.path.splitext(base)
    if len(base) > 200:
        base = stem[: 200 - len(ext)] + ext
    return base or "file"


def truncate(text: str, limit: int) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"


def as_id_list(value: Any) -> list[int]:
    if not value:
        return []
    return [int(value)] if isinstance(value, int) else [int(v) for v in value]


def remove_quiet(*paths: str) -> None:
    for p in paths:
        try:
            os.remove(p)
        except FileNotFoundError:
            pass


def verify_size(path: str, expected: Optional[int]) -> None:
    actual = os.path.getsize(path)
    if expected is not None and actual != expected:
        raise ValueError(f"size mismatch: expected {expected}, got {actual}")


def download_file(url: str, dest: str, expected_size: Optional[int]) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(dest)), exist_ok=True)
    last_err: Optional[Exception] = None
    for attempt in range(1, DOWNLOAD_ATTEMPTS + 1):
        remove_quiet(dest, dest + ".aria2")
        if shutil.which("aria2c"):
            try:
                subprocess.run(
                    [
                        "aria2c", "-x", "16", "-s", "16", "-k", "1M",
                        "--file-allocation=none", "--allow-overwrite=true",
                        "--auto-file-renaming=false", "--max-tries=5", "--retry-wait=3",
                        "--connect-timeout=30", "--timeout=60",
                        "--console-log-level=error", "--summary-interval=0",
                        "-d", os.path.dirname(os.path.abspath(dest)),
                        "-o", os.path.basename(dest), url,
                    ],
                    check=True, timeout=DOWNLOAD_TIMEOUT_S, stdin=subprocess.DEVNULL,
                )
                verify_size(dest, expected_size)
                return
            except (subprocess.SubprocessError, OSError, ValueError) as e:
                log(f"  -> aria2c failed ({type(e).__name__}); falling back to requests")
                remove_quiet(dest, dest + ".aria2")
        try:
            with requests.get(url, stream=True, timeout=HTTP_TIMEOUT) as r:
                r.raise_for_status()
                with open(dest, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
            verify_size(dest, expected_size)
            return
        except (requests.RequestException, OSError, ValueError) as e:
            last_err = e
            log(f"  -> download attempt {attempt}/{DOWNLOAD_ATTEMPTS} failed: {type(e).__name__}")
            time.sleep(3 * attempt)
    remove_quiet(dest, dest + ".aria2")
    raise RuntimeError(f"download failed after {DOWNLOAD_ATTEMPTS} attempts: {last_err}")


def create_git_bundle(clone_url: str, bundle_path: str) -> Optional[str]:
    """Returns short HEAD sha, or None if the repository is empty."""
    repo_dir = tempfile.mkdtemp(prefix="bare_", suffix=".git", dir=WORK_DIR)
    try:
        subprocess.run(
            ["git", "-c", "http.lowSpeedLimit=1000", "-c", "http.lowSpeedTime=300",
             "clone", "--bare", "--quiet", clone_url, repo_dir],
            check=True, env=GIT_ENV, timeout=GIT_TIMEOUT_S, stdin=subprocess.DEVNULL,
        )
        head = subprocess.run(
            ["git", "-C", repo_dir, "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, env=GIT_ENV, timeout=60,
        )
        if head.returncode != 0 or not head.stdout.strip():
            return None
        subprocess.run(
            ["git", "-C", repo_dir, "bundle", "create", os.path.abspath(bundle_path), "--all"],
            check=True, env=GIT_ENV, timeout=GIT_TIMEOUT_S,
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
        )
        return head.stdout.strip()
    finally:
        shutil.rmtree(repo_dir, ignore_errors=True)


# ============================================================================
# TELEGRAM HELPERS
# ============================================================================
async def tg_call(factory: Callable[[], Awaitable[T]], attempts: int = 6) -> T:
    delay = 2
    failures = 0
    while True:
        try:
            return await factory()
        except FloodWaitError as e:
            log(f"Telegram flood wait. Sleeping {e.seconds + 2}s...")
            await asyncio.sleep(e.seconds + 2)
        except TRANSIENT as e:
            failures += 1
            if failures >= attempts:
                raise
            log(f"Transient Telegram error ({type(e).__name__}); retry in {delay}s")
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)


async def create_topic(chat_id: int, title: str) -> int:
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/createForumTopic"
    payload = {"chat_id": chat_id, "name": truncate(title, TOPIC_TITLE_MAX)}
    failures = 0
    while True:
        try:
            r = await asyncio.to_thread(requests.post, url, json=payload, timeout=HTTP_TIMEOUT)
            data = r.json()
        except (requests.RequestException, ValueError) as e:
            failures += 1
            if failures >= 5:
                raise RuntimeError(f"createForumTopic network failure: {type(e).__name__}") from None
            await asyncio.sleep(2 ** failures)
            continue
        if data.get("ok"):
            return int(data["result"]["message_thread_id"])
        if data.get("error_code") == 429:
            wait = int(data.get("parameters", {}).get("retry_after", 10)) + 2
            log(f"Topic creation rate-limited. Waiting {wait}s...")
            await asyncio.sleep(wait)
            continue
        raise RuntimeError(f"Failed to create topic '{title}': {data.get('description', data)}")


async def resolve_group(client: TelegramClient, group_id: int | str) -> Any:
    try:
        return await client.get_entity(group_id)
    except ValueError:
        # Bots start with an empty entity cache; resolve supergroups with access_hash=0.
        if isinstance(group_id, int) and str(group_id).startswith("-100"):
            res = await client(GetChannelsRequest([InputChannel(int(str(group_id)[4:]), 0)]))
            if res.chats:
                return res.chats[0]
        raise


# ============================================================================
# MULTI-SOCKET PARALLEL MTPROTO UPLOADER
# ============================================================================
class ParallelUploader:
    """Several MTProto sockets sharing the session auth key; falls back to the main client."""

    def __init__(self, client: TelegramClient, api_id: int, sockets: int):
        self.client = client
        self.api_id = api_id
        self.sockets = sockets
        self.senders: list[Optional[MTProtoSender]] = []
        self._started = False
        self._lock = asyncio.Lock()

    async def _open_sender(self) -> MTProtoSender:
        dc = await self.client._get_dc(self.client.session.dc_id)
        sender = MTProtoSender(self.client.session.auth_key, loggers=self.client._log)
        try:
            await sender.connect(
                self.client._connection(
                    dc.ip_address, dc.port, dc.id,
                    loggers=self.client._log, proxy=self.client._proxy,
                )
            )
            init = InvokeWithLayerRequest(
                LAYER,
                InitConnectionRequest(
                    api_id=self.api_id,
                    device_model="GitHubActionsArchiver",
                    system_version="Linux",
                    app_version="2.0",
                    system_lang_code="en",
                    lang_pack="",
                    lang_code="en",
                    query=GetConfigRequest(),
                ),
            )
            await asyncio.wait_for(sender.send(init), 30)
        except Exception:
            try:
                await sender.disconnect()
            except Exception:
                pass
            raise
        return sender

    async def start(self) -> None:
        async with self._lock:
            if self._started:
                return
            results = await asyncio.gather(
                *(self._open_sender() for _ in range(self.sockets)), return_exceptions=True
            )
            opened: list[Optional[MTProtoSender]] = []
            for res in results:
                if isinstance(res, BaseException):
                    log(f"Upload socket skipped: {type(res).__name__}: {res}")
                else:
                    opened.append(res)
            self.senders = opened or [None]
            self._started = True
            log(f"⚡ {len(opened)} parallel upload sockets ready (DC{self.client.session.dc_id})")

    async def _send_part(
        self, slot: int, file_id: int, idx: int, count: int, chunk: bytes, is_big: bool
    ) -> None:
        failures = 0
        while True:
            sender = self.senders[slot]
            req = (
                SaveBigFilePartRequest(file_id, idx, count, chunk)
                if is_big
                else SaveFilePartRequest(file_id, idx, chunk)
            )
            try:
                pending = self.client(req) if sender is None else sender.send(req)
                ok = await asyncio.wait_for(pending, PART_TIMEOUT_S)
                if ok is False:
                    raise ConnectionError("part rejected by server")
                return
            except FloodWaitError as e:
                await asyncio.sleep(e.seconds + 1)
            except (RPCError, OSError, asyncio.TimeoutError) as exc:
                failures += 1
                is_rpc = isinstance(exc, RPCError)
                fatal = is_rpc and not isinstance(exc, ServerError) and sender is None
                if fatal or failures >= PART_RETRIES:
                    raise
                if sender is not None and (is_rpc or failures >= 2):
                    self.senders[slot] = None
                await asyncio.sleep(min(2 ** failures, 30))

    async def _upload_once(
        self, path: str, offset: int, length: int, name: str
    ) -> InputFile | InputFileBig:
        await self.start()
        part_count = math.ceil(length / PART_SIZE)
        is_big = length > BIG_FILE_THRESHOLD
        file_id = helpers.generate_random_long()
        queue: asyncio.Queue[int] = asyncio.Queue()
        for i in range(part_count):
            queue.put_nowait(i)
        began = time.monotonic()

        with open(path, "rb") as fh:

            async def worker(slot: int) -> None:
                while True:
                    try:
                        idx = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        return
                    start = offset + idx * PART_SIZE
                    size = min(PART_SIZE, offset + length - start)
                    fh.seek(start)  # no await between seek and read: atomic in the event loop
                    chunk = fh.read(size)
                    if len(chunk) != size:
                        raise IOError(f"short read at part {idx} of {path}")
                    await self._send_part(slot, file_id, idx, part_count, chunk, is_big)

            tasks = [
                asyncio.create_task(worker(slot))
                for slot in range(len(self.senders))
                for _ in range(INFLIGHT_PER_SOCKET)
            ]
            try:
                await asyncio.gather(*tasks)
            except BaseException:
                for t in tasks:
                    t.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                raise

        elapsed = max(time.monotonic() - began, 1e-6)
        log(f"  -> uploaded {format_size(length)} in {elapsed:.1f}s ({length / elapsed / 1048576:.1f} MB/s)")
        if is_big:
            return InputFileBig(file_id, part_count, name)
        return InputFile(file_id, part_count, name, "")

    async def upload(
        self, path: str, *, offset: int = 0, length: Optional[int] = None, name: Optional[str] = None
    ) -> InputFile | InputFileBig:
        total = os.path.getsize(path)
        length = total - offset if length is None else length
        if length <= 0:
            raise ValueError(f"nothing to upload: {path}")
        if length > MAX_UPLOAD_BYTES:
            raise ValueError(f"chunk exceeds {MAX_UPLOAD_BYTES} bytes")
        name = name or os.path.basename(path)
        last: Optional[Exception] = None
        for attempt in range(1, 4):
            try:
                return await self._upload_once(path, offset, length, name)
            except (RPCError, OSError, asyncio.TimeoutError) as e:
                last = e
                log(f"  -> upload attempt {attempt}/3 failed: {type(e).__name__}: {e}")
                await asyncio.sleep(5 * attempt)
        raise RuntimeError(f"upload failed: {last}")

    async def close(self) -> None:
        for s in self.senders:
            if s is not None:
                try:
                    await s.disconnect()
                except Exception:
                    pass
        self.senders.clear()
        self._started = False


# ============================================================================
# ARCHIVING LOGIC
# ============================================================================
@dataclass
class Ctx:
    client: TelegramClient
    uploader: ParallelUploader
    group: Any
    chat_id: int
    db: dict
    changed: bool = False
    failures: list[str] = field(default_factory=list)

    def fail(self, msg: str) -> None:
        msg = truncate(msg.replace("\n", " "), 500)
        self.failures.append(msg)
        log(f"::warning::{msg}")

    def persist(self) -> None:
        save_db(self.db)
        self.changed = True


def build_caption(
    icon: str, title: str, tag: str, size: int, sha: str, part: Optional[tuple[int, int, str]] = None
) -> str:
    lines = [
        f"{icon} <b>{html.escape(truncate(title, TITLE_MAX))}</b>",
        f"🏷️ <b>Version:</b> <code>{html.escape(truncate(tag, 100))}</code>  |  "
        f"📦 <b>Size:</b> <code>{format_size(size)}</code>",
        f"🔒 <b>SHA-256{' (whole file)' if part else ''}:</b> <code>{sha}</code>",
    ]
    if part:
        idx, total, fname = part
        lines.append(
            f"🧩 <b>Part {idx} of {total}</b> (<code>{html.escape(truncate(fname, 150))}</code>)"
        )
    return "\n".join(lines)


async def send_document(ctx: Ctx, topic_id: int, handle: Any, caption: str) -> int:
    msg = await tg_call(
        lambda: ctx.client.send_file(
            ctx.group, handle, caption=caption, parse_mode="html",
            force_document=True, reply_to=topic_id,
        )
    )
    return msg.id


async def upload_file_or_split(
    ctx: Ctx, topic_id: int, file_path: str, title: str, tag_name: str, icon: Optional[str] = None
) -> list[int]:
    size = os.path.getsize(file_path)
    if size == 0:
        log(f"  -> skipping empty file {os.path.basename(file_path)}")
        return []
    sha = await asyncio.to_thread(sha256_hex, file_path)
    icon = icon or get_file_icon(file_path)
    base = os.path.basename(file_path)

    if size <= MAX_UPLOAD_BYTES:
        handle = await ctx.uploader.upload(file_path, name=base)
        return [await send_document(ctx, topic_id, handle, build_caption(icon, title, tag_name, size, sha))]

    part_total = math.ceil(size / MAX_UPLOAD_BYTES)
    sent: list[int] = []
    try:
        for i in range(part_total):
            offset = i * MAX_UPLOAD_BYTES
            length = min(MAX_UPLOAD_BYTES, size - offset)
            part_name = f"{base}.{i + 1:03d}"
            handle = await ctx.uploader.upload(file_path, offset=offset, length=length, name=part_name)
            caption = build_caption(icon, title, tag_name, size, sha, (i + 1, part_total, part_name))
            sent.append(await send_document(ctx, topic_id, handle, caption))
    except BaseException:
        if sent:
            try:
                await ctx.client.delete_messages(ctx.group, sent)
            except Exception:
                pass
        raise
    return sent


def header_html(repo: dict) -> str:
    owner = repo["owner"]
    description = repo.get("description") or "No description provided."
    language = repo.get("language") or "N/A"
    license_name = (repo.get("license") or {}).get("spdx_id") or "Custom/None"
    return (
        f"📦 <b>{html.escape(repo['name'])}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📝 <b>Info:</b> {html.escape(description)}\n"
        f"⭐ <b>Stars:</b> <code>{repo.get('stargazers_count', 0):,}</code>  |  "
        f"💻 <b>Lang:</b> <code>{html.escape(language)}</code>  |  "
        f"⚖️ <b>License:</b> <code>{html.escape(license_name)}</code>\n\n"
        f"👤 <b>Creator:</b> <a href=\"{html.escape(owner['html_url'], quote=True)}\">"
        f"{html.escape(owner['login'])}</a>\n"
        f"🔗 <b>GitHub Repo:</b> <a href=\"{html.escape(repo['html_url'], quote=True)}\">"
        f"{html.escape(repo['full_name'])}</a>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📌 <i>Official Automated Backup Topic</i>"
    )


async def ensure_project(ctx: Ctx, repo: dict) -> dict:
    full_name = repo["full_name"]
    proj = ctx.db["projects"].get(full_name)

    if proj is None or not proj.get("topic_id"):
        log(f"Creating dedicated topic for {full_name}...")
        topic_id = await create_topic(ctx.chat_id, f"🕹️ {repo['name']} ({repo['owner']['login']})")
        proj = {
            "name": repo["name"],
            "owner": repo["owner"]["login"],
            "owner_url": repo["owner"]["html_url"],
            "repo_url": repo["html_url"],
            "description": repo.get("description") or "No description provided.",
            "topic_id": topic_id,
            "header_msg_id": None,
            "latest_bundle_msg_id": None,
            "last_pushed_at": None,
            "releases": {},
        }
        ctx.db["projects"][full_name] = proj
        ctx.persist()

    proj.setdefault("releases", {})
    proj.setdefault("latest_bundle_msg_id", None)
    proj.setdefault("last_pushed_at", None)

    if not proj.get("header_msg_id"):
        text = header_html(repo)
        header_msg = await tg_call(
            lambda: ctx.client.send_message(
                ctx.group, text, parse_mode="html", reply_to=proj["topic_id"], link_preview=False
            )
        )
        proj["header_msg_id"] = header_msg.id
        ctx.persist()
        try:
            await tg_call(lambda: ctx.client.pin_message(ctx.group, header_msg.id, notify=False))
        except Exception as e:
            log(f"Could not pin header in {repo['name']}: {type(e).__name__}")
    return proj


async def backup_releases(ctx: Ctx, proj: dict, full_name: str) -> None:
    topic_id = proj["topic_id"]
    releases = await asyncio.to_thread(get_repo_releases, full_name)

    for rel in releases:
        if rel.get("draft"):
            continue
        rel_id = str(rel["id"])
        rec = proj["releases"].get(rel_id)
        if rec is not None and rec.get("complete", True):
            continue

        tag_name = rel.get("tag_name") or "untagged"
        rel_name = rel.get("name") or tag_name
        published = (rel.get("published_at") or "N/A")[:10]

        if rec is None:
            body = (rel.get("body") or "No changelog provided.").strip()
            if len(body) > CHANGELOG_MAX:
                body = body[:CHANGELOG_MAX] + "\n... [Changelog truncated]"
            changelog = (
                f"🚀 <b>Release: {html.escape(truncate(rel_name, 200))}</b>\n"
                f"📅 <b>Published:</b> <code>{published}</code>  |  "
                f"🏷️ <b>Tag:</b> <code>{html.escape(truncate(tag_name, 100))}</code>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📋 <b>Changelog:</b>\n"
                f"<blockquote>{html.escape(body)}</blockquote>"
            )
            msg = await tg_call(
                lambda: ctx.client.send_message(
                    ctx.group, changelog, parse_mode="html", reply_to=topic_id, link_preview=False
                )
            )
            rec = {
                "tag_name": tag_name,
                "name": rel_name,
                "published_at": published,
                "changelog_msg_id": msg.id,
                "assets": {},
                "complete": False,
            }
            proj["releases"][rel_id] = rec
            ctx.persist()

        all_ok = True
        for asset in rel.get("assets", []):
            asset_name = asset["name"]
            if asset_name in rec["assets"]:
                continue
            if asset.get("state", "uploaded") != "uploaded":
                all_ok = False
                continue

            work = os.path.join(WORK_DIR, f"asset_{asset['id']}")
            path = os.path.join(work, safe_filename(asset_name))
            try:
                log(f"  -> [aria2c 16x] Downloading {asset_name}...")
                await asyncio.to_thread(
                    download_file, asset["browser_download_url"], path, asset.get("size")
                )
                log(f"  -> [Multi-Socket] Uploading {asset_name}...")
                rec["assets"][asset_name] = await upload_file_or_split(
                    ctx, topic_id, path, asset_name, tag_name
                )
                ctx.persist()
            except Exception as e:
                all_ok = False
                ctx.fail(f"{full_name} {tag_name} asset '{asset_name}': {type(e).__name__}: {e}")
            finally:
                shutil.rmtree(work, ignore_errors=True)

        if all_ok:
            rec["complete"] = True
            ctx.persist()


async def backup_bundle(ctx: Ctx, proj: dict, repo: dict) -> None:
    full_name = repo["full_name"]
    pushed_at = repo.get("pushed_at")
    if proj.get("last_pushed_at") == pushed_at:
        return

    log(f"Updating latest full Git bundle for {full_name}...")
    work = tempfile.mkdtemp(prefix="bundle_", dir=WORK_DIR)
    bundle_path = os.path.join(work, safe_filename(f"{repo['name']}-full-history.bundle"))
    try:
        head_sha = await asyncio.to_thread(create_git_bundle, repo["clone_url"], bundle_path)
        if head_sha is None:
            log(f"  -> {full_name} is empty; nothing to bundle")
            proj["last_pushed_at"] = pushed_at
            ctx.persist()
            return

        old_ids = as_id_list(proj.get("latest_bundle_msg_id"))
        new_ids = await upload_file_or_split(
            ctx,
            proj["topic_id"],
            bundle_path,
            f"Latest Full Source & Git History (Commit #{head_sha} — Includes All Past Tags)",
            f"HEAD @ {(pushed_at or 'N/A')[:10]}",
            icon="🧬",
        )
        proj["latest_bundle_msg_id"] = new_ids
        proj["last_pushed_at"] = pushed_at
        ctx.persist()

        if old_ids:  # delete the previous bundle only after the new one is safely stored
            try:
                await tg_call(lambda: ctx.client.delete_messages(ctx.group, old_ids))
            except Exception as e:
                log(f"  -> could not delete old bundle: {type(e).__name__}")
    except Exception as e:
        ctx.fail(f"{full_name} git bundle: {type(e).__name__}: {e}")
    finally:
        shutil.rmtree(work, ignore_errors=True)


async def process_repo(ctx: Ctx, repo: dict) -> None:
    full_name = repo["full_name"]
    proj = await ensure_project(ctx, repo)
    try:
        await backup_releases(ctx, proj, full_name)
    except Exception as e:
        ctx.fail(f"{full_name} releases: {type(e).__name__}: {e}")
    await backup_bundle(ctx, proj, repo)


async def publish_db(ctx: Ctx) -> None:
    db = ctx.db
    if not db.get("db_topic_id"):
        db["db_topic_id"] = await create_topic(ctx.chat_id, "🗄️ 00_ARCHIVE_DATABASE")
        save_db(db)
    caption = (
        f"🔄 <b>Updated Archive Database</b>\n"
        f"📊 <b>Projects Backed Up:</b> <code>{len(db['projects'])}</code>\n"
        f"ℹ️ <i>Feed this file to the restoration script to rebuild any nuked repo.</i>"
    )
    await tg_call(
        lambda: ctx.client.send_file(
            ctx.group, DB_FILE, caption=caption, parse_mode="html",
            force_document=True, reply_to=db["db_topic_id"],
        )
    )


async def main() -> int:
    shutil.rmtree(WORK_DIR, ignore_errors=True)
    os.makedirs(WORK_DIR, exist_ok=True)
    if not GH_TOKEN:
        log("::warning::GH_TOKEN not set; GitHub API limited to 60 requests/hour")

    db = load_db()
    save_db(db)

    client = TelegramClient(
        StringSession(), TG_API_ID, TG_API_HASH,
        connection_retries=10, retry_delay=3, request_retries=10,
        timeout=30, receive_updates=False,
    )
    client.flood_sleep_threshold = 3600
    await client.start(bot_token=TG_BOT_TOKEN)

    uploader = ParallelUploader(client, TG_API_ID, UPLOAD_SOCKETS)
    ctx: Optional[Ctx] = None
    try:
        group = await resolve_group(client, TG_GROUP_ID)
        if not getattr(group, "forum", False):
            log("::warning::Target group does not report forum topics enabled")
        ctx = Ctx(client, uploader, group, get_peer_id(group), db)

        starred = await asyncio.to_thread(get_starred_repos, GH_USERNAME)
        log(f"Found {len(starred)} starred repositories.")

        for repo in starred:
            try:
                await process_repo(ctx, repo)
            except Exception as e:
                ctx.fail(f"{repo.get('full_name', '?')}: {type(e).__name__}: {e}")
    finally:
        if ctx is not None and ctx.changed:
            try:
                await publish_db(ctx)
            except Exception as e:
                log(f"::warning::Could not publish database: {type(e).__name__}: {e}")
        await uploader.close()
        await client.disconnect()
        shutil.rmtree(WORK_DIR, ignore_errors=True)

    if ctx and ctx.failures:
        log(f"Completed with {len(ctx.failures)} failure(s); they will be retried next run.")
        return 1 if STRICT_EXIT else 0
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
