#!/usr/bin/env python3
"""
Forward DiscordChatExporter JSON to a Discord webhook, with:
- Robust retry/backoff (429 Retry-After + 5xx with jitter)
- Download-or-link attachment policy (cap by --max-attach-mb)
- Payload splitting (text chunks + attachment batches)
- Reply mapping when possible; fallback manual quote
- Dry-run & verbose logging
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Any, Tuple, Optional
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

import requests

# ----------------- Small utils -----------------

def iso_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()

def load_json(path: str, default):
    if not path or not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def save_json(path: str, data: Any):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def clamp_webhook_username(u: str) -> str:
    return (u or "")[:80]

def ensure_query_param(url: str, key: str, value: str) -> str:
    u = urlparse(url)
    q = dict(parse_qsl(u.query))
    if q.get(key) != value:
        q[key] = value
        return urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(q), u.fragment))
    return url

def first_n(s: str, n: int) -> str:
    s = s or ""
    return (s[:n] + "…") if len(s) > n else s

def chunk_text(s: str, limit: int = 1900) -> List[str]:
    """Split text under Discord 2000 cap; leaves headroom for prefixes."""
    s = s or ""
    if not s:
        return [""]
    out: List[str] = []
    while s:
        out.append(s[:limit])
        s = s[limit:]
    return out

def sanitize_filename(fn: str) -> str:
    # Windows-forbidden chars and control chars
    fn = re.sub(r"[\\/:*?\"<>|\x00-\x1F]", "_", fn or "file")
    if len(fn) > 180:
        base, ext = os.path.splitext(fn)
        fn = base[:170] + "~" + ext[:9]
    if not fn:
        fn = "file"
    return fn

# ----------------- Export normalization -----------------

def extract_reply_reference(msg: Dict[str, Any]) -> Optional[str]:
    # Try multiple exporter shapes
    ref = msg.get("reference")
    if isinstance(ref, dict):
        m = ref.get("messageId") or ref.get("message_id") or ref.get("id")
        if m: return str(m)

    r = msg.get("referencedMessage")
    if isinstance(r, dict):
        m = r.get("id")
        if m: return str(m)

    r2 = msg.get("repliesTo") or msg.get("replies_to")
    if isinstance(r2, dict):
        m = r2.get("id")
        if m: return str(m)

    return None

def normalize_export(obj: Any) -> List[Dict[str, Any]]:
    """
    Accepts DiscordChatExporter JSON (list or dict) and returns a uniform list.
    Each item has: id, timestamp, content, username, avatar_url, attachments,
    embeds, reply_to_id, reply_preview, jump_url
    """
    msgs = obj.get("messages") if isinstance(obj, dict) else obj
    if not isinstance(msgs, list):
        return []

    norm: List[Dict[str, Any]] = []
    for msg in msgs:
        m_id = str(msg.get("id") or "")
        ts = msg.get("timestamp") or msg.get("Timestamp") or msg.get("timestampISO") or ""
        content = msg.get("content") or msg.get("Content") or ""
        author = msg.get("author") or {}
        username = author.get("name") or author.get("username") or "Unknown"
        avatar_url = author.get("avatarUrl") or author.get("avatar") or None

        # attachments: try exporter shapes
        attachments: List[Dict[str, Any]] = []
        atts = msg.get("attachments") or msg.get("Attachments") or []
        if isinstance(atts, list):
            for a in atts:
                url = a.get("url") or a.get("Url") or a.get("proxyUrl")
                if not url: 
                    continue
                attachments.append({
                    "url": url,
                    "filename": sanitize_filename(a.get("fileName") or a.get("filename") or os.path.basename(url)),
                    "content_type": a.get("contentType") or a.get("type") or None,
                    "size_hint": a.get("size") or 0,
                })

        # embeds (if you want to carry across simple link embeds)
        embeds = msg.get("embeds") or []
        if not isinstance(embeds, list):
            embeds = []

        reply_to = extract_reply_reference(msg)
        ref_preview = None
        if reply_to:
            # Build a tiny local preview if exporter included referencedMessage
            ref_msg = msg.get("referencedMessage")
            if isinstance(ref_msg, dict):
                rauthor = (ref_msg.get("author") or {}).get("name") or "Unknown"
                rcontent = first_n(ref_msg.get("content") or "", 120)
                ref_preview = f"Replying to {rauthor}: “{rcontent}”"

        norm.append({
            "id": m_id,
            "timestamp": ts,
            "content": content,
            "username": clamp_webhook_username(username),
            "avatar_url": avatar_url,
            "attachments": attachments,
            "embeds": embeds,
            "reply_to_id": reply_to,
            "reply_preview": ref_preview,
            "jump_url": msg.get("url") or msg.get("jumpUrl") or None,
        })
    return norm

# ----------------- HTTP with retry/backoff -----------------

def session_with_retries() -> requests.Session:
    s = requests.Session()
    # Small pool; we do manual retry to respect 429 Retry-After precisely.
    s.headers.update({"User-Agent": "discord-forwarder/1.1"})
    return s

def _sleep_backoff(i: int, base: float = 0.8, cap: float = 10.0):
    # exponential backoff with jitter
    t = min(cap, base * (2 ** i)) + (0.05 * i)
    time.sleep(t)

def post_webhook(session: requests.Session, webhook_url: str, payload: Dict[str, Any],
                 files: Optional[List[Tuple[str, Tuple[str, bytes, str]]]] = None,
                 verbose: bool = False) -> requests.Response:
    wh = ensure_query_param(webhook_url, "wait", "true")
    tries = 0
    last_resp: Optional[requests.Response] = None
    while True:
        try:
            if files:
                # payload_json for multipart uploads
                resp = session.post(wh, data={"payload_json": json.dumps(payload, ensure_ascii=False)},
                                    files=files, timeout=90)
            else:
                resp = session.post(wh, json=payload, timeout=45)
        except Exception as e:
            if verbose:
                print(f"[http] post error: {e}; retrying...", file=sys.stderr)
            last_resp = None
            if tries >= 5:
                raise
            _sleep_backoff(tries)
            tries += 1
            continue

        if resp.status_code == 429:
            retry_after = float(resp.headers.get("Retry-After", "1"))
            if verbose:
                print(f"[http] 429; sleeping {retry_after:.2f}s", file=sys.stderr)
            time.sleep(max(0.2, retry_after))
            tries += 1
            if tries > 8:
                return resp
            continue

        if 500 <= resp.status_code < 600:
            if verbose:
                print(f"[http] {resp.status_code}; retrying...", file=sys.stderr)
            if tries >= 5:
                return resp
            _sleep_backoff(tries)
            tries += 1
            last_resp = resp
            continue

        return resp

def head_file(session: requests.Session, url: str) -> Optional[requests.Response]:
    try:
        return session.head(url, timeout=20, allow_redirects=True)
    except Exception:
        return None

def get_file(session: requests.Session, url: str) -> Optional[requests.Response]:
    tries = 0
    while True:
        try:
            r = session.get(url, timeout=90, stream=False)
        except Exception:
            r = None
        if r and 200 <= r.status_code < 300:
            return r
        if r and r.status_code == 429:
            ra = float(r.headers.get("Retry-After", "1"))
            time.sleep(max(0.2, ra))
        elif not r or 500 <= (r.status_code if r else 500) < 600:
            if tries >= 4:
                return r
            _sleep_backoff(tries)
        else:
            return r
        tries += 1

# ----------------- Forwarding core -----------------

def forward_message(session: requests.Session, webhook_url: str, message: Dict[str, Any],
                    id_map: Dict[str, str], size_cap: int, max_files_per_post: int,
                    dry_run: bool, verbose: bool) -> Optional[str]:
    """
    Returns dest message id (first post) or None.
    """
    # Build content with optional manual quote if reply mapping not resolvable
    content = message["content"] or ""
    reply_to_id = message.get("reply_to_id")
    dest_reply_id = id_map.get(reply_to_id) if reply_to_id else None

    header = ""
    if reply_to_id and not dest_reply_id and message.get("reply_preview"):
        header = f"{message['reply_preview']}\n"
    elif reply_to_id and not dest_reply_id:
        header = f"(replying to an earlier message)\n"

    chunks = chunk_text(header + content, 1900)

    base_payload: Dict[str, Any] = {
        "content": f"{chunks[0]} (part 1)" if len(chunks) > 1 else chunks[0],
        "username": message["username"],
    }
    if message.get("avatar_url"):
        base_payload["avatar_url"] = message["avatar_url"]
    if dest_reply_id:
        base_payload["message_reference"] = {"message_id": dest_reply_id, "fail_if_not_exists": False}

    # Decide attachments: download or link
    downloadable: List[Tuple[str, bytes, str, str]] = []  # (filename, content, ctype, src_url)
    link_only: List[str] = []

    for att in message["attachments"]:
        url = att["url"]
        fn = sanitize_filename(att["filename"])
        ctype = att.get("content_type") or "application/octet-stream"
        size_hint = int(att.get("size_hint") or 0)

        if not size_hint:
            hr = head_file(session, url)
            try:
                if hr and 200 <= hr.status_code < 300:
                    size_hint = int(hr.headers.get("Content-Length") or "0")
            except Exception:
                size_hint = 0

        if size_hint and size_hint > size_cap:
            link_only.append(url)
            continue

        gr = get_file(session, url)
        if not gr or not (200 <= gr.status_code < 300):
            link_only.append(url)
            continue

        content_bytes = gr.content
        if len(content_bytes) > size_cap:
            link_only.append(url)
            continue

        downloadable.append((fn, content_bytes, ctype, url))

    # Link summary for any link-only attachments
    link_suffix = ""
    if link_only:
        link_suffix = "\n" + "\n".join(f"Attachment: {u}" for u in link_only)

    # Send base post
    if dry_run:
        print(f"[dry-run] Would POST content len={len(base_payload['content'])}, files={min(len(downloadable), max_files_per_post)} (+{len(link_only)} links)")
        return None

    dest_message_id: Optional[str] = None

    if downloadable:
        # First post can include up to max_files_per_post files
        first_batch = downloadable[:max_files_per_post]
        files_form = [(f"files[{i}]", (fn, blob, ctype)) for i, (fn, blob, ctype, _url) in enumerate(first_batch)]
        payload = dict(base_payload)
        payload["content"] = (payload["content"] + link_suffix) if link_suffix else payload["content"]

        resp = post_webhook(session, webhook_url, payload, files_form, verbose)
        try:
            dest_message_id = resp.json().get("id")
        except Exception:
            dest_message_id = None

        # Remaining batches
        remaining = downloadable[max_files_per_post:]
        batch_idx = 2
        while remaining:
            batch = remaining[:max_files_per_post]
            remaining = remaining[max_files_per_post:]

            follow: Dict[str, Any] = {"content": f"(attachment batch {batch_idx})", "username": message["username"]}
            if message.get("avatar_url"):
                follow["avatar_url"] = message["avatar_url"]
            if dest_message_id:
                follow["message_reference"] = {"message_id": dest_message_id, "fail_if_not_exists": False}

            files_form = [(f"files[{i}]", (fn, blob, ctype)) for i, (fn, blob, ctype, _u) in enumerate(batch)]
            post_webhook(session, webhook_url, follow, files_form, verbose)
            batch_idx += 1
    else:
        # No downloadable files – just text (and link_suffix)
        payload = dict(base_payload)
        payload["content"] = payload["content"] + link_suffix

        resp = post_webhook(session, webhook_url, payload, None, verbose)
        try:
            dest_message_id = resp.json().get("id")
        except Exception:
            dest_message_id = None

    # Extra text chunks as chained replies
    prev = dest_message_id
    for idx, extra in enumerate(chunks[1:], start=2):
        extra_payload = {"content": f"{extra} (part {idx})", "username": message["username"]}
        if message.get("avatar_url"):
            extra_payload["avatar_url"] = message["avatar_url"]
        if prev:
            extra_payload["message_reference"] = {"message_id": prev, "fail_if_not_exists": False}
        r = post_webhook(session, webhook_url, extra_payload, None, verbose)
        try:
            prev = r.json().get("id") or prev
        except Exception:
            pass

    return dest_message_id

# ----------------- CLI -----------------

def main():
    ap = argparse.ArgumentParser(description="Forward exported Discord messages to a webhook (once).")
    ap.add_argument("--json", required=True, help="Path to DiscordChatExporter JSON")
    ap.add_argument("--webhook", required=True, help="Destination Discord webhook URL")
    ap.add_argument("--state", required=True, help="Path to state file to track seen IDs")
    ap.add_argument("--id-map", required=True, help="Path to ID map (src_id -> dest_id) for replies")
    ap.add_argument("--max-attach-mb", type=float, default=7.8, help="Max per-file upload size")
    ap.add_argument("--max-files-per-post", type=int, default=8, help="Attachment batch size per message (<=10)")
    ap.add_argument("--dry-run", action="store_true", help="Do not post; print actions instead")
    ap.add_argument("--verbose", action="store_true", help="Verbose logs")
    args = ap.parse_args()

    # Load JSON
    try:
        with open(args.json, "r", encoding="utf-8") as f:
            export_obj = json.load(f)
    except Exception as e:
        print(f"[error] Failed to read JSON: {e}", file=sys.stderr)
        sys.exit(2)

    messages = normalize_export(export_obj)

    state = load_json(args.state, default={})
    seen: Dict[str, str] = state.get("seen_ids") or {}
    id_map: Dict[str, str] = load_json(args.id_map, default={})

    # Filter only new
    new_msgs = [m for m in messages if m["id"] not in seen]
    if args.verbose:
        print(f"[info] messages_total={len(messages)} new={len(new_msgs)} seen={len(seen)}")

    size_cap = int(args.max_attach_mb * 1024 * 1024)
    session = session_with_retries()

    sent = 0
    for m in new_msgs:
        try:
            dest_id = forward_message(session, args.webhook, m, id_map, size_cap,
                                      max(1, min(10, args.max_files_per_post)),
                                      dry_run=args.dry_run, verbose=args.verbose)
            seen[m["id"]] = iso_z(datetime.now(timezone.utc))
            if dest_id:
                id_map[m["id"]] = dest_id
            sent += 1
        except Exception as e:
            print(f"[error] Failed to forward id={m['id']}: {e}", file=sys.stderr)

    state["seen_ids"] = seen
    if not args.dry_run:
        save_json(args.state, state)
        save_json(args.id_map, id_map)

    print(f"[forward] processed={len(messages)} forwarded_new={sent}{' (dry-run)' if args.dry_run else ''}")

if __name__ == "__main__":
    main()
