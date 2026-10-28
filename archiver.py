import os
import html
import json
import shutil
import hashlib
import asyncio
import requests
import subprocess
from telethon import TelegramClient
from telethon.utils import get_peer_id
from telethon.errors import FloodWaitError

# --- CONFIGURATION FROM GITHUB SECRETS ---
GH_USERNAME = os.environ["GH_USERNAME"].strip()
GH_TOKEN = os.environ.get("GH_TOKEN", "").strip()
TG_API_ID = int(os.environ["TG_API_ID"].strip())
TG_API_HASH = os.environ["TG_API_HASH"].strip()
TG_BOT_TOKEN = os.environ["TG_BOT_TOKEN"].strip()

raw_group_id = os.environ["TG_GROUP_ID"].strip()
TG_GROUP_ID = int(raw_group_id) if raw_group_id.lstrip("-").isdigit() else raw_group_id

DB_FILE = "database.json"
WORK_DIR = "temp_downloads"

gh_headers = {"Accept": "application/vnd.github+json"}
if GH_TOKEN:
    gh_headers["Authorization"] = f"Bearer {GH_TOKEN}"


def load_db():
    """Automatically creates a fresh database in memory if database.json doesn't exist yet."""
    if os.path.exists(DB_FILE):
        with open(DB_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"db_topic_id": None, "projects": {}}


def save_db(db):
    with open(DB_FILE, "w", encoding="utf-8") as f:
        json.dump(db, f, indent=2, ensure_ascii=False)


def format_size(size_bytes):
    for unit in ["B", "KB", "MB", "GB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} TB"


def get_sha256_short(file_path):
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(1024 * 1024):
            h.update(chunk)
    return h.hexdigest()[:16]


def get_file_icon(filename):
    ext = filename.lower()
    if ext.endswith(".apk"):
        return "🤖"
    if ext.endswith((".exe", ".msi")):
        return "🪟"
    if ext.endswith((".dmg", ".pkg")):
        return "🍎"
    if ext.endswith((".appimage", ".deb", ".rpm", ".flatpak", ".tar.gz")):
        return "🐧"
    if ext.endswith(".bundle"):
        return "🧬"
    return "💿"


def get_starred_repos(username):
    repos = []
    page = 1
    while True:
        url = f"https://api.github.com/users/{username}/starred?per_page=100&page={page}"
        r = requests.get(url, headers=gh_headers)
        r.raise_for_status()
        data = r.json()
        if not data:
            break
        repos.extend(data)
        page += 1
    return list(reversed(repos))


def get_repo_releases(full_name):
    releases = []
    page = 1
    while True:
        url = f"https://api.github.com/repos/{full_name}/releases?per_page=100&page={page}"
        r = requests.get(url, headers=gh_headers)
        r.raise_for_status()
        data = r.json()
        if not data:
            break
        releases.extend(data)
        page += 1
    return list(reversed(releases))


def download_file(url, dest_path):
    with requests.get(url, headers=gh_headers, stream=True) as r:
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192 * 16):
                f.write(chunk)


def create_git_bundle(clone_url, bundle_path):
    repo_dir = os.path.join(WORK_DIR, "bare_repo.git")
    if os.path.exists(repo_dir):
        shutil.rmtree(repo_dir)
    subprocess.run(["git", "clone", "--mirror", clone_url, repo_dir], check=True)
    head_sha = subprocess.check_output(
        ["git", "-C", repo_dir, "rev-parse", "--short", "HEAD"]
    ).decode().strip()
    subprocess.run(
        ["git", "-C", repo_dir, "bundle", "create", os.path.abspath(bundle_path), "--all"],
        check=True
    )
    shutil.rmtree(repo_dir)
    return head_sha


async def safe_tg_call(coro):
    while True:
        try:
            return await coro
        except FloodWaitError as e:
            print(f"Telegram rate limit hit. Waiting {e.seconds + 2}s...")
            await asyncio.sleep(e.seconds + 2)


async def create_topic(chat_id, title):
    """Creates a Forum Topic using the official Bot API (immune to Telethon TL version changes)."""
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/createForumTopic"
    while True:
        r = requests.post(url, json={"chat_id": chat_id, "name": title[:120]})
        data = r.json()
        if data.get("ok"):
            return data["result"]["message_thread_id"]
        if data.get("error_code") == 429:
            retry_after = data.get("parameters", {}).get("retry_after", 10)
            print(f"Topic creation rate-limited. Waiting {retry_after + 2}s...")
            await asyncio.sleep(retry_after + 2)
        else:
            raise RuntimeError(f"Failed to create topic '{title}': {data}")


async def upload_file_or_split(client, group_entity, topic_id, file_path, title_html, tag_name):
    max_size = 1950 * 1024 * 1024  # 1.95 GB safe limit per file
    file_size = os.path.getsize(file_path)
    size_str = format_size(file_size)
    sha_short = get_sha256_short(file_path)
    icon = get_file_icon(file_path)

    caption = (
        f"{icon} <b>{title_html}</b>\n"
        f"🏷️ <b>Version:</b> <code>{html.escape(tag_name)}</code>  |  📦 <b>Size:</b> <code>{size_str}</code>\n"
        f"🔒 <b>SHA-256:</b> <code>{sha_short}...</code>"
    )

    if file_size <= max_size:
        msg = await safe_tg_call(
            client.send_file(
                group_entity,
                file_path,
                caption=caption,
                parse_mode="html",
                reply_to=topic_id
            )
        )
        await asyncio.sleep(3)
        return [msg.id]
    else:
        split_prefix = file_path + ".part"
        subprocess.run(["7z", "a", "-v1900m", "-mx0", f"{split_prefix}.7z", file_path], check=True)
        msg_ids = []
        folder = os.path.dirname(file_path)
        parts = sorted([f for f in os.listdir(folder) if f.startswith(os.path.basename(split_prefix))])
        for idx, fname in enumerate(parts, 1):
            part_path = os.path.join(folder, fname)
            part_caption = f"{caption}\n🧩 <b>Part {idx} of {len(parts)}</b> (<code>{html.escape(fname)}</code>)"
            msg = await safe_tg_call(
                client.send_file(
                    group_entity,
                    part_path,
                    caption=part_caption,
                    parse_mode="html",
                    reply_to=topic_id
                )
            )
            msg_ids.append(msg.id)
            os.remove(part_path)
            await asyncio.sleep(3)
        return msg_ids


async def main():
    os.makedirs(WORK_DIR, exist_ok=True)
    db = load_db()
    save_db(db)  # Ensure database.json exists on disk immediately

    client = TelegramClient("bot_session", TG_API_ID, TG_API_HASH)
    await client.start(bot_token=TG_BOT_TOKEN)

    # Resolve group entity once at startup so Telethon caches its access_hash
    group_entity = await client.get_entity(TG_GROUP_ID)
    bot_api_chat_id = get_peer_id(group_entity)

    starred_repos = get_starred_repos(GH_USERNAME)
    print(f"Found {len(starred_repos)} starred repositories.")

    db_updated = False

    for repo in starred_repos:
        full_name = repo["full_name"]
        repo_name = repo["name"]
        owner_login = repo["owner"]["login"]
        owner_url = repo["owner"]["html_url"]
        repo_url = repo["html_url"]
        description = repo.get("description") or "No description provided."
        language = repo.get("language") or "N/A"
        stars_count = repo.get("stargazers_count", 0)
        license_name = (repo.get("license") or {}).get("spdx_id") or "Custom/None"
        pushed_at = repo.get("pushed_at")

        # 1. Create Dedicated Project Topic & First Pinned Header Message
        if full_name not in db["projects"]:
            print(f"Creating dedicated topic for {full_name}...")
            topic_id = await create_topic(bot_api_chat_id, f"🕹️ {repo_name} ({owner_login})")

            header_html = (
                f"📦 <b>{html.escape(repo_name)}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📝 <b>Info:</b> {html.escape(description)}\n"
                f"⭐ <b>Stars:</b> <code>{stars_count:,}</code>  |  "
                f"💻 <b>Lang:</b> <code>{html.escape(language)}</code>  |  "
                f"⚖️ <b>License:</b> <code>{html.escape(license_name)}</code>\n\n"
                f"👤 <b>Creator:</b> <a href=\"{owner_url}\">{html.escape(owner_login)}</a>\n"
                f"🔗 <b>GitHub Repo:</b> <a href=\"{repo_url}\">{html.escape(full_name)}</a>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📌 <i>Official Automated Backup Topic</i>"
            )

            header_msg = await safe_tg_call(
                client.send_message(
                    group_entity,
                    header_html,
                    parse_mode="html",
                    reply_to=topic_id,
                    link_preview=False
                )
            )

            try:
                await safe_tg_call(client.pin_message(group_entity, header_msg.id, notify=False))
            except Exception as e:
                print(f"Could not pin header in {repo_name}: {e}")

            db["projects"][full_name] = {
                "name": repo_name,
                "owner": owner_login,
                "owner_url": owner_url,
                "repo_url": repo_url,
                "description": description,
                "topic_id": topic_id,
                "header_msg_id": header_msg.id,
                "latest_bundle_msg_id": None,
                "last_pushed_at": None,
                "releases": {}
            }
            save_db(db)
            db_updated = True
            await asyncio.sleep(2)

        proj = db["projects"][full_name]
        topic_id = proj["topic_id"]

        # 2. Check & Backup All Releases (Oldest to Newest)
        releases = get_repo_releases(full_name)
        for rel in releases:
            rel_id = str(rel["id"])
            if rel_id in proj["releases"]:
                continue

            tag_name = rel.get("tag_name") or "untagged"
            rel_name = rel.get("name") or tag_name
            published_date = (rel.get("published_at") or "N/A")[:10]
            raw_body = (rel.get("body") or "No changelog provided.").strip()

            if len(raw_body) > 3000:
                raw_body = raw_body[:3000] + "\n... [Changelog truncated]"

            changelog_html = (
                f"🚀 <b>Release: {html.escape(rel_name)}</b>\n"
                f"📅 <b>Published:</b> <code>{published_date}</code>  |  🏷️ <b>Tag:</b> <code>{html.escape(tag_name)}</code>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📋 <b>Changelog:</b>\n"
                f"<blockquote>{html.escape(raw_body)}</blockquote>"
            )

            changelog_msg = await safe_tg_call(
                client.send_message(
                    group_entity,
                    changelog_html,
                    parse_mode="html",
                    reply_to=topic_id,
                    link_preview=False
                )
            )

            rel_record = {
                "tag_name": tag_name,
                "name": rel_name,
                "published_at": published_date,
                "changelog_msg_id": changelog_msg.id,
                "source_zip_msg_id": None,
                "assets": {}
            }

            # 2a. Source Code Zip for this specific version
            if rel.get("zipball_url"):
                src_zip_name = f"{repo_name}-{tag_name}-source.zip".replace("/", "_")
                src_zip_path = os.path.join(WORK_DIR, src_zip_name)
                try:
                    download_file(rel["zipball_url"], src_zip_path)
                    msg_ids = await upload_file_or_split(
                        client,
                        group_entity,
                        topic_id,
                        src_zip_path,
                        f"Source Code Snapshot ({html.escape(src_zip_name)})",
                        tag_name
                    )
                    rel_record["source_zip_msg_id"] = msg_ids
                except Exception as e:
                    print(f"  -> Failed to backup source zip for {tag_name}: {e}")
                finally:
                    if os.path.exists(src_zip_path):
                        os.remove(src_zip_path)

            # 2b. All Release Binaries (.apk, .exe, .dmg, .zip, etc.)
            for asset in rel.get("assets", []):
                asset_name = asset["name"]
                asset_url = asset["browser_download_url"]
                asset_path = os.path.join(WORK_DIR, asset_name)

                print(f"  -> Downloading {asset_name}...")
                try:
                    download_file(asset_url, asset_path)
                    msg_ids = await upload_file_or_split(
                        client,
                        group_entity,
                        topic_id,
                        asset_path,
                        html.escape(asset_name),
                        tag_name
                    )
                    rel_record["assets"][asset_name] = msg_ids
                except Exception as e:
                    print(f"  -> Failed to backup asset {asset_name}: {e}")
                finally:
                    if os.path.exists(asset_path):
                        os.remove(asset_path)

            proj["releases"][rel_id] = rel_record
            save_db(db)
            db_updated = True

        # 3. Full Git History Bundle (Only runs if new commits were pushed since last check)
        if proj.get("last_pushed_at") != pushed_at:
            print(f"Updating full Git bundle for {full_name}...")
            bundle_name = f"{repo_name}-full-history.bundle"
            bundle_path = os.path.join(WORK_DIR, bundle_name)
            try:
                head_sha = create_git_bundle(repo["clone_url"], bundle_path)
                msg_ids = await upload_file_or_split(
                    client,
                    group_entity,
                    topic_id,
                    bundle_path,
                    f"Full Git Repository Bundle (Commit #{head_sha})",
                    f"HEAD @ {(pushed_at or 'N/A')[:10]}"
                )
                proj["latest_bundle_msg_id"] = msg_ids
                proj["last_pushed_at"] = pushed_at
                save_db(db)
                db_updated = True
            except Exception as e:
                print(f"  -> Could not create Git bundle for {full_name}: {e}")
            finally:
                if os.path.exists(bundle_path):
                    os.remove(bundle_path)

    # 4. Upload Updated Database File to the Dedicated Database Topic
    if db_updated:
        if not db.get("db_topic_id"):
            db["db_topic_id"] = await create_topic(bot_api_chat_id, "🗄️ 00_ARCHIVE_DATABASE")
            save_db(db)

        await safe_tg_call(
            client.send_file(
                group_entity,
                DB_FILE,
                caption=(
                    f"🔄 <b>Updated Archive Database</b>\n"
                    f"📊 <b>Projects Backed Up:</b> <code>{len(db['projects'])}</code>\n"
                    f"ℹ️ <i>Feed this file to the restoration script to rebuild any nuked repo.</i>"
                ),
                parse_mode="html",
                reply_to=db["db_topic_id"]
            )
        )

    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
