import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Dict, List, Any, Tuple, Optional
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

import requests

# ----------------- Helpers -----------------

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

def iso_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()

def clamp_webhook_username(u: str) -> str:
    return (u or "")[:80]

def ensure_query_param(webhook_url: str, key: str, value: str) -> str:
    """
    Ensure webhook_url has a particular query param. Discord ignores unknown params,
    but this is used to toggle threads_passive=false for easily replying later.
    We keep it stable so idempotent posts don't bounce back."""
    u = urlparse(webhook_url)
    q = dict(parse_qsl(u.query))
    if q.get(key) != value:
        q[key] = value
        return urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(q), u.fragment))
    return webhook_url

def first_n(s: str, n: int) -> str:
    s = s or ""
    return (s[:n] + "…") if len(s) > n else s

# Added helpers: chunking + author/index for manual quote lookup
def chunk_text(s: str, limit: int = 1900) -> List[str]:
    """
    Split text into safe chunks below Discord's 2000-char cap.
    Keep headroom for quotes/prefixes.
    """
    s = s or ""
    if not s:
        return [""]
    out = []
    i = 0
    while i < len(s):
        out.append(s[i:i+limit])
        i += limit
    return out

def author_name(author: dict) -> str:
    if not isinstance(author, dict):
        return "Unknown"
    return author.get("nickname") or author.get("name") or author.get("username") or "Unknown"

def build_id_index(messages: list) -> Dict[str, Dict[str, Any]]:
    idx = {}
    for m in messages or []:
        mid = str(m.get("id") or "")
        if not mid:
            continue
        try:
            content = (m.get("content") or "").strip()
        except Exception:
            content = ""
        try:
            a = author_name(m.get("author") or {})
        except Exception:
            a = "Unknown"
        idx[mid] = {"author": a, "content": content, "timestamp": m.get("timestamp")}
    return idx

# ----------------- Export normalization -----------------

def extract_reply_reference(msg: Dict[str, Any]) -> Optional[str]:
    # Try exporter styles
    # 1) 'reference' style
    ref = msg.get("reference")
    if isinstance(ref, dict):
        # direct reply reference style
        m = ref.get("messageId") or ref.get("message_id") or ref.get("id")
        if m:
            return str(m)
    # 2) 'referencedMessage' style
    r = msg.get("referencedMessage")
    if isinstance(r, dict):
        m = r.get("id")
        if m:
            return str(m)
    # 3) some exports store 'repliesTo'
    r2 = msg.get("repliesTo") or msg.get("replies_to")
    if isinstance(r2, dict):
        m = r2.get("id")
        if m:
            return str(m)
    # 4) none
    return None

def batch_list(lst: list, n: int) -> List[list]:
    return [lst[i:i+n] for i in range(0, len(lst), n)]

def build_payload_base(message: Dict[str, Any]) -> Dict[str, Any]:
    payload = {
        "content": message["content"] or "",
        "username": message["username"],
    }
    if message.get("avatar_url"):
        payload["avatar_url"] = message["avatar_url"]
    if message.get("embeds"):
        try:
            json.dumps(message["embeds"])
            payload["embeds"] = message["embeds"]
        except Exception:
            pass
    return payload

def normalize_message(msg: Dict[str, Any]) -> Dict[str, Any]:
    # Some exporter formats vary; normalize to a minimal shape we use later.
    m_id = str(msg.get("id"))
    ts = msg.get("timestamp")
    content = msg.get("content") or ""
    author = msg.get("author") or {}
    username = author.get("nickname") or author.get("name") or author.get("username") or "Unknown"
    avatar_url = author.get("avatarUrl") or author.get("avatar_url") or None

    # attachments (download URLs + filename + contentType)
    attachments = []
    for a in msg.get("attachments") or []:
        url = a.get("url") or a.get("proxy_url")
        if url:
            size_hint = a.get("size") or a.get("fileSize") or 0
            attachments.append({
                "url": url,
                "filename": a.get("fileName") or a.get("filename") or os.path.basename(url),
                "content_type": a.get("contentType") or a.get("content_type") or "application/octet-stream",
                "size_hint": size_hint
            })

    embeds = msg.get("embeds") or []

    # Try to capture a tiny snapshot of the referenced message if present in export
    refmsg = msg.get("referencedMessage")
    ref_preview = None
    if isinstance(refmsg, dict):
        rauthor = (refmsg.get("author") or {})
        rname = rauthor.get("nickname") or rauthor.get("name") or "Unknown"
        rcontent = refmsg.get("content") or ""
        rts = refmsg.get("timestamp")
        ref_preview = {
            "id": str(refmsg.get("id")) if refmsg.get("id") else None,
            "author": rname,
            "content": first_n(rcontent.strip(), 180),
            "timestamp": rts
        }

    return {
        "id": m_id,
        "timestamp": ts,
        "content": content,
        "username": username[:80],  # webhook username cap
        "avatar_url": avatar_url,
        "attachments": attachments,
        "embeds": embeds,
        "reply_to_id": extract_reply_reference(msg),
        "reply_preview": ref_preview,  # may be None
        "jump_url": msg.get("url") or msg.get("jumpUrl") or None,
    }

# ----------------- HTTP -----------------

def post_webhook(session: requests.Session, webhook_url: str, payload: Dict[str, Any], files: Optional[List[Tuple[str, Tuple[str, bytes, str]]]] = None) -> requests.Response:
    wh = ensure_query_param(webhook_url, "wait", "true")
    headers = {"Content-Type": "application/json"} if not files else None
    if files:
        return session.post(wh, data={"payload_json": json.dumps(payload, ensure_ascii=False)}, files=files, timeout=60)
    return session.post(wh, json=payload, timeout=30)

def head_file(session: requests.Session, url: str) -> Optional[requests.Response]:
    try:
        return session.head(url, timeout=15, allow_redirects=True)
    except Exception:
        return None

def get_file(session: requests.Session, url: str) -> Optional[requests.Response]:
    try:
        return session.get(url, timeout=60, stream=False)
    except Exception:
        return None

# ----------------- Forwarding -----------------

def forward_one_message(
    session: requests.Session,
    webhook_url: str,
    message: Dict[str, Any],
    max_attach_mb: float,
    max_files_per_post: int,
    id_map: Dict[str, str],
    id_index: Optional[Dict[str, Dict[str, Any]]] = None,
) -> str:
    """
    Forward a single exporter message.
    Returns the destination message ID (for id_map).
    """
    size_cap = int(max_attach_mb * 1024 * 1024)

    # Build base payload
    base_payload = build_payload_base(message)

    # If this is a reply and we know the parent's DEST id, create a real reply.
    # Otherwise, prepend a manual quote line as fallback.
    dest_parent_id = None
    source_parent_id = message.get("reply_to_id")
    if source_parent_id:
        dest_parent_id = id_map.get(source_parent_id)
        if dest_parent_id:
            base_payload["message_reference"] = {
                "message_id": dest_parent_id,
                "fail_if_not_exists": False
            }
        else:
            # Build a manual one-line quote preview
            qp = message.get("reply_preview") or {}
            # If exporter didn't embed a preview, try to look up the parent in id_index
            if (not qp) and id_index and source_parent_id and source_parent_id in id_index:
                looked = id_index.get(source_parent_id) or {}
                qp = {
                    "author": looked.get("author") or "Unknown",
                    "content": looked.get("content") or "",
                    "timestamp": looked.get("timestamp"),
                }
            qp_author = (qp.get("author") or "unknown").strip()
            qp_content = (qp.get("content") or "").strip()
            qp_line = f'> Quote {qp_author}: "{first_n(qp_content, 180)}"'
            if message.get("jump_url"):
                qp_line += f"\n> {message['jump_url']}"
            content = (base_payload.get("content") or "").strip()
            base_payload["content"] = (qp_line + ("\n\n" + content if content else "")).strip()

    # Decide per attachment whether to re-upload or just link
    downloadable_files: List[Tuple[str, bytes, str, str]] = []  # (filename, content, content_type, source_url)
    link_only_urls: List[str] = []
    for att in message["attachments"]:
        url = att["url"]
        filename = att["filename"]
        ctype = att.get("content_type") or "application/octet-stream"

        # HEAD to get size if not provided
        size_hint = att.get("size_hint") or 0
        try:
            if not size_hint:
                hr = head_file(session, url)
                if hr and (hr.status_code // 100) == 2:
                    size_hint = int(hr.headers.get("Content-Length") or "0")
        except Exception:
            size_hint = 0

        if size_hint and size_hint > size_cap:
            link_only_urls.append(url)
            continue

        # Try to GET (re-upload)
        gr = get_file(session, url)
        if not gr or (gr.status_code // 100) != 2:
            link_only_urls.append(url)
            continue
        content = gr.content
        if len(content) > size_cap:
            link_only_urls.append(url)
            continue

        downloadable_files.append((filename, content, ctype, url))

    if link_only_urls:
        links_text = "\n".join(f"[Attachment too large] {u}" for u in link_only_urls)
        base_payload["content"] = (base_payload.get("content") or "").strip()
        base_payload["content"] = (base_payload["content"] + ("\n" if base_payload["content"] else "") + links_text).strip()

    # --- Chunk long content safely
    content_full = base_payload.get("content") or ""
    chunks = chunk_text(content_full, 1900)
    base_payload["content"] = chunks[0]

    # --- Empty-message handling
    content_now = (base_payload.get("content") or "").strip()
    embeds_now = base_payload.get("embeds") or []
    has_files = bool(downloadable_files)
    has_links = bool(link_only_urls)
    if (not content_now) and (not embeds_now) and (not has_files) and (not has_links):
        base_payload["content"] = "[no text]"

    # Send text + first batch of files
    files_form = []
    dest_message_id = None
    if downloadable_files:
        first_batch = downloadable_files[:max_files_per_post]
        for idx, (fn, content, ctype, _url) in enumerate(first_batch):
            files_form.append((f"files[{idx}]", (fn, content, ctype)))
        resp = post_webhook(session, webhook_url, base_payload, files_form)
        try:
            dest_message_id = resp.json().get("id")
        except Exception:
            dest_message_id = None

        # Post remaining text chunks as chained replies (if any)
        prev_id = dest_message_id
        for idx_chunk, extra in enumerate(chunks[1:], start=2):
            extra_payload = {"content": f"{extra} (part {idx_chunk})", "username": message["username"]}
            if message.get("avatar_url"):
                extra_payload["avatar_url"] = message["avatar_url"]
            if prev_id:
                extra_payload["message_reference"] = {"message_id": prev_id, "fail_if_not_exists": False}
            resp2 = post_webhook(session, webhook_url, extra_payload, files=None)
            try:
                prev_id = resp2.json().get("id") or prev_id
            except Exception:
                pass

        # Any remaining files → additional posts (optionally as replies to first message if we got its ID)
        remaining = downloadable_files[max_files_per_post:]
        if remaining:
            for batch_idx, batch in enumerate(batch_list(remaining, max_files_per_post), start=1):
                follow_payload = {
                    "content": f"(attachment batch {batch_idx}/{(len(remaining)+max_files_per_post-1)//max_files_per_post}) from original message {message['id']}",
                    "username": message["username"],
                }
                if message.get("avatar_url"):
                    follow_payload["avatar_url"] = message["avatar_url"]
                if dest_message_id:
                    follow_payload["message_reference"] = {
                        "message_id": dest_message_id,
                        "fail_if_not_exists": False
                    }
                files_form = []
                for idx, (fn, content, ctype, _url) in enumerate(batch):
                    files_form.append((f"files[{idx}]", (fn, content, ctype)))
                post_webhook(session, webhook_url, follow_payload, files_form)
    else:
        # No files → single post
        resp = post_webhook(session, webhook_url, base_payload, None)
        try:
            dest_message_id = resp.json().get("id")
        except Exception:
            dest_message_id = None

        # Post remaining text chunks as chained replies (if any)
        prev_id = dest_message_id
        for idx_chunk, extra in enumerate(chunks[1:], start=2):
            extra_payload = {"content": f"{extra} (part {idx_chunk})", "username": message["username"]}
            if message.get("avatar_url"):
                extra_payload["avatar_url"] = message["avatar_url"]
            if prev_id:
                extra_payload["message_reference"] = {"message_id": prev_id, "fail_if_not_exists": False}
            resp2 = post_webhook(session, webhook_url, extra_payload, files=None)
            try:
                prev_id = resp2.json().get("id") or prev_id
            except Exception:
                pass

    return dest_message_id or ""

# ----------------- Main -----------------

def main():
    ap = argparse.ArgumentParser(description="Forward new messages from a DiscordChatExporter JSON to a Discord webhook with replies/quotes and attachment size caps.")
    ap.add_argument("--webhook", required=True, help="Destination Discord webhook URL")
    ap.add_argument("--json", required=True, help="Path to the JSON file exported this run")
    ap.add_argument("--state", required=True, help="Path to persistent state.json for dedupe")
    ap.add_argument("--id-map", default="id_map.json", help="Path to persistent source->dest message ID map (per channel recommended)")
    ap.add_argument("--max-attach-mb", type=float, default=25.0, help="Max size to re-upload (MB). Too-large files are posted as links.")
    ap.add_argument("--max-files-per-post", type=int, default=10, help="Max files per webhook post (Discord limit = 10).")
    args = ap.parse_args()

    if not os.path.exists(args.json):
        print(f"[error] JSON file not found: {args.json}", file=sys.stderr)
        sys.exit(1)

    # Load exporter JSON
    try:
        with open(args.json, "r", encoding="utf-8") as f:
            exported = json.load(f)
    except Exception as e:
        print(f"[error] Failed to read JSON {args.json}: {e}", file=sys.stderr)
        sys.exit(1)

    msgs = exported.get("messages") if isinstance(exported, dict) else exported
    if not isinstance(msgs, list):
        print("[error] Unexpected exporter JSON structure (missing 'messages' array).", file=sys.stderr)
        sys.exit(1)

    # Build id index for manual quote lookups
    id_index = build_id_index(msgs)

    normalized = [normalize_message(m) for m in msgs if m.get("id")]
    normalized.sort(key=lambda m: (m.get("timestamp") or "", m["id"]))

    # Load persistent state: seen source IDs (dedupe)
    state = load_json(args.state, {})
    seen: Dict[str, str] = state.get("seen_ids") or {}

    # Load id_map: source message id -> dest message id (for real reply threading)
    id_map: Dict[str, str] = load_json(args.id_map, {})

    # Session
    sess = requests.Session()
    # Ensure we get a synchronous response so we can read new dest IDs
    webhook = ensure_query_param(args.webhook, "wait", "true")

    sent_count = 0
    for m in normalized:
        # Deduplicate by source id
        if m["id"] in seen:
            continue

        try:
            dest_id = forward_one_message(
                session=sess,
                webhook_url=webhook,
                message=m,
                max_attach_mb=args.max_attach_mb,
                max_files_per_post=args.max_files_per_post,
                id_map=id_map,
                id_index=id_index,
            )
            # Mark as seen regardless (avoid repeats). Save mapping if we got a dest id.
            seen[m["id"]] = iso_z(datetime.now(timezone.utc))
            if dest_id:
                id_map[m["id"]] = dest_id
            sent_count += 1
        except Exception as e:
            print(f"[error] Failed to forward id={m['id']}: {e}", file=sys.stderr)

    state["seen_ids"] = seen
    save_json(args.state, state)
    save_json(args.id_map, id_map)
    print(f"[forward] processed={len(normalized)} forwarded_new={sent_count}")

if __name__ == "__main__":
    main()
