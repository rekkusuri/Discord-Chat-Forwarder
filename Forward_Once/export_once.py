#!/usr/bin/env python3
"""
Export a window with DiscordChatExporter and immediately forward it with forward_once.py.
- Auto-resume using per-channel state
- Edge overlap window to avoid boundary misses (dedupe in forwarder)
- Pass-through of upload size caps / file batch size
- Verbose logging & dry-run support
"""

import argparse
import datetime
import json
import os
import subprocess
import sys
from pathlib import Path

def iso(dt: datetime.datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt.isoformat()

def parse_iso(s: str) -> datetime.datetime:
    s = s.strip()
    try:
        if len(s) == 10 and s[4] == "-" and s[7] == "-":
            return datetime.datetime.fromisoformat(s + "T00:00:00")
        return datetime.datetime.fromisoformat(s)
    except Exception:
        raise ValueError(f"Invalid date/ISO string: {s}")

def read_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def write_json(path: Path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    tmp.replace(path)

def find_latest_msg_ts_in_export(file_path: Path) -> datetime.datetime | None:
    try:
        data = read_json(file_path)
        msgs = data.get("messages") if isinstance(data, dict) else data
        if not isinstance(msgs, list) or not msgs:
            return None
        latest = None
        for m in msgs:
            ts = m.get("timestamp") or m.get("Timestamp") or m.get("timestampISO")
            if not ts:
                continue
            try:
                dt = datetime.datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except Exception:
                continue
            latest = dt if latest is None else max(latest, dt)
        return latest
    except Exception:
        return None

def scan_exports_for_latest(channel_id: str, export_dir: Path) -> datetime.datetime | None:
    best = None
    for p in export_dir.glob("*.json"):
        dt = find_latest_msg_ts_in_export(p)
        if dt:
            best = dt if best is None else max(best, dt)
    return best

def load_state(state_dir: Path, channel_id: str) -> dict:
    p = state_dir / f"channel_{channel_id}.json"
    if p.exists():
        try:
            return read_json(p)
        except Exception:
            pass
    return {}

def save_state(state_dir: Path, channel_id: str, last_exported_iso: str):
    p = state_dir / f"channel_{channel_id}.json"
    write_json(p, {"last_exported_iso": last_exported_iso})

def main():
    ap = argparse.ArgumentParser(description="Export once and forward.")
    ap.add_argument("--token", required=True, help="Discord token for DiscordChatExporter")
    ap.add_argument("--channel", required=True, help="Channel ID to export")
    ap.add_argument("--guild", default="", help="Optional Guild ID (for nicer file name)")
    ap.add_argument("--webhook", required=True, help="Destination Discord webhook")

    ap.add_argument("--export-dir", default="exports", help="Where to save exporter JSON")
    ap.add_argument("--state-dir", default="state", help="Folder to store per-channel resume state")
    ap.add_argument("--exporter-path", default=r".\DiscordChatExporter.Cli.exe",
                    help="Path to DiscordChatExporter.Cli.exe")
    ap.add_argument("--forwarder-path", default="", help="Path to forward_once.py (default: alongside this script)")

    ap.add_argument("--since", default="", help="Override start (YYYY-MM-DD or ISO). If omitted, auto-resume.")
    ap.add_argument("--until", default="", help="End (YYYY-MM-DD or ISO). Default = now")
    ap.add_argument("--edge-overlap-seconds", type=int, default=60,
                    help="Expand export window on both sides to avoid boundary misses.")

    # Forwarding passthrough
    ap.add_argument("--max-attach-mb", type=float, default=7.8, help="Max per-file upload size")
    ap.add_argument("--max-files-per-post", type=int, default=8, help="Max files per webhook post (<=10)")
    ap.add_argument("--dry-run", action="store_true", help="Forwarder dry-run (export still runs)")
    ap.add_argument("--verbose", action="store_true", help="Verbose logging")

    args = ap.parse_args()

    export_dir = Path(args.export_dir); export_dir.mkdir(parents=True, exist_ok=True)
    state_dir = Path(args.state_dir); state_dir.mkdir(parents=True, exist_ok=True)
    exporter = Path(args.exporter_path)
    if not exporter.exists():
        print(f"[error] Exporter not found at: {exporter}", file=sys.stderr); sys.exit(2)

    forwarder_py = Path(args.forwarder_path) if args.forwarder_path else Path(__file__).with_name("forward_once.py")

    # Determine window
    until_dt = parse_iso(args.until) if args.until else datetime.datetime.now(datetime.timezone.utc)
    since_dt: datetime.datetime
    if args.since:
        since_dt = parse_iso(args.since)
    else:
        st = load_state(state_dir, args.channel)
        last_iso = st.get("last_exported_iso")
        if last_iso:
            try:
                since_dt = datetime.datetime.fromisoformat(last_iso)
            except Exception:
                since_dt = None
        else:
            since_dt = None

        if not since_dt:
            guess = scan_exports_for_latest(args.channel, export_dir)
            if guess:
                since_dt = guess
            else:
                # default: 14 days lookback on first run (safe)
                since_dt = until_dt - datetime.timedelta(days=14)

    # Apply overlap
    overlap = datetime.timedelta(seconds=max(0, args.edge_overlap_seconds))
    since_eff = since_dt - overlap
    until_eff = until_dt + overlap

    # Build filename
    guild_part = f"{args.guild}_" if args.guild else ""
    out_name = f"{guild_part}{args.channel}_{since_eff.strftime('%Y%m%dT%H%M%S')}_{until_eff.strftime('%Y%m%dT%H%M%S')}.json"
    out_path = export_dir / out_name

    if args.verbose:
        print(f"[info] Exporting: channel={args.channel} since={since_eff.isoformat()} until={until_eff.isoformat()}")
        print(f"[info] -> {out_path}")

    # Run exporter
    # DiscordChatExporter CLI typical args:
    # DiscordChatExporter.Cli.exe export -t <token> -c <channel> -f Json -o <file> --after <iso> --before <iso>
    cmd = [
        str(exporter),
        "export",
        "-t", args.token,
        "-c", args.channel,
        "-f", "Json",
        "-o", str(out_path),
        "--after", iso(since_eff),
        "--before", iso(until_eff),
    ]
    if args.verbose:
        print("[info] Running exporter (token redacted):", " ".join([cmd[0]] + cmd[1:3] + ["***"] + cmd[4:]))

    r = subprocess.run(cmd, capture_output=True, text=True)
    if args.verbose:
        sys.stdout.write(r.stdout)
    if r.returncode != 0:
        sys.stderr.write(r.stderr)
        print("[error] Export failed.", file=sys.stderr)
        sys.exit(r.returncode)

    # Find latest message timestamp in the exported file
    latest_dt = find_latest_msg_ts_in_export(out_path)
    if not latest_dt and args.verbose:
        print("[warn] Could not detect latest timestamp in export; forwarding anyway.")

    # Forward
    fwd_state = state_dir / f"forward_state_{args.channel}.json"
    fwd_idmap = state_dir / f"id_map_{args.channel}.json"

    fwd_cmd = [
        sys.executable, str(forwarder_py),
        "--webhook", args.webhook,
        "--json", str(out_path),
        "--state", str(fwd_state),
        "--id-map", str(fwd_idmap),
        "--max-attach-mb", str(args.max_attach_mb),
        "--max-files-per-post", str(args.max_files_per_post),
    ]
    if args.dry_run:
        fwd_cmd.append("--dry-run")
    if args.verbose:
        fwd_cmd.append("--verbose")

    if args.verbose:
        print("[info] Forwarding (webhook redacted):", " ".join(fwd_cmd[:3] + ["***"] + fwd_cmd[4:]))

    r2 = subprocess.run(fwd_cmd, capture_output=True, text=True)
    sys.stdout.write(r2.stdout)
    if r2.returncode != 0:
        sys.stderr.write(r2.stderr)
        print("[error] Forwarding failed.", file=sys.stderr)
        sys.exit(r2.returncode)

    # Update state (use actual latest if available, else until_eff)
    final_dt = latest_dt or until_eff
    save_state(state_dir, args.channel, iso(final_dt))
    if args.verbose:
        print(f"[info] Updated state last_exported_iso = {iso(final_dt)}")

    print("[done] Exported and forwarded once.")

if __name__ == "__main__":
    main()
