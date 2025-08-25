# export_once.py
import argparse, json, os, sys, glob, subprocess, datetime
from pathlib import Path

# ---------- Helpers ----------
def iso(dt: datetime.datetime) -> str:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=datetime.timezone.utc).isoformat()
    return dt.isoformat()

def parse_iso(s: str) -> datetime.datetime:
    # Accept plain date or ISO with/without timezone
    s = s.strip()
    try:
        if len(s) == 10 and s[4] == "-" and s[7] == "-":
            # YYYY-MM-DD -> midnight local naive
            return datetime.datetime.fromisoformat(s + "T00:00:00")
        return datetime.datetime.fromisoformat(s)
    except Exception:
        raise ValueError(f"Invalid ISO/date string: {s}")

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
        # messages from DiscordChatExporter have 'timestamp' as ISO
        latest = None
        for m in msgs:
            ts = m.get("timestamp")
            if not ts:
                continue
            try:
                dt = parse_iso(ts)
            except Exception:
                continue
            latest = dt if latest is None else max(latest, dt)
        return latest
    except Exception:
        return None

def scan_exports_for_latest(channel_id: str, export_dir: Path) -> datetime.datetime | None:
    # Look for any JSONs that contain this channel (by filename convention or all jsons)
    candidates = []
    for p in export_dir.glob("*.json"):
        name = p.name.lower()
        if channel_id in name or True:
            candidates.append(p)
    best = None
    for p in candidates:
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
    state = {"channel_id": channel_id, "last_exported_iso": last_exported_iso}
    write_json(p, state)

# ---------- Core ----------
def main():
    ap = argparse.ArgumentParser(description="Export once & forward for one channel with resume-by-date.")
    ap.add_argument("--channel", required=True, help="Discord channel ID")
    ap.add_argument("--webhook", required=True, help="Destination webhook URL")
    ap.add_argument("--token", required=True, help="Discord user/bot token for exporter")
    ap.add_argument("--export-dir", default="exports", help="Folder to write JSON export")
    ap.add_argument("--state-dir", default="state", help="Folder to store per-channel resume state")
    ap.add_argument("--exporter-path", default=r".\DiscordChatExporter.Cli.exe",
                    help="Path to DiscordChatExporter.Cli.exe")
    ap.add_argument("--since", default="", help="Override start (YYYY-MM-DD or ISO). If omitted, auto-resume.")
    ap.add_argument("--until", default="", help="End (YYYY-MM-DD or ISO). Default = now")
    ap.add_argument("--filename", default="", help="Optional output filename. Default is auto.")
    ap.add_argument("--max-attach-mb", type=float, default=7.8, help="Pass-through to forward_new.py")
    ap.add_argument("--max-files-per-post", type=int, default=8, help="Pass-through to forward_new.py")
    ap.add_argument("--edge-overlap-seconds", type=int, default=60,
                help="Expand export window on both sides to avoid boundary misses (dedupe in forwarder prevents dupes).")

    args = ap.parse_args()

    export_dir = Path(args.export_dir); export_dir.mkdir(parents=True, exist_ok=True)
    state_dir = Path(args.state_dir); state_dir.mkdir(parents=True, exist_ok=True)
    exporter = Path(args.exporter_path)
    if not exporter.exists():
        print(f"[error] Exporter not found at: {exporter}", file=sys.stderr); sys.exit(2)

    # ---- Determine effective window
    # UNTIL: default now
    if args.until:
        until_dt = parse_iso(args.until)
    else:
        until_dt = datetime.datetime.now(datetime.timezone.utc)

    # SINCE: explicit > state > scan
    since_dt = None
    if args.since:
        since_dt = parse_iso(args.since)
    else:
        state = load_state(state_dir, args.channel)
        last_iso = state.get("last_exported_iso")
        if last_iso:
            try:
                since_dt = parse_iso(last_iso) + datetime.timedelta(seconds=1)
            except Exception:
                since_dt = None
        if since_dt is None:
            scanned = scan_exports_for_latest(args.channel, export_dir)
            if scanned:
                since_dt = scanned + datetime.timedelta(seconds=1)

    OVERLAP = datetime.timedelta(seconds=args.edge_overlap_seconds)
    if since_dt:
        since_dt = since_dt - OVERLAP
    if until_dt:
        until_dt = until_dt + OVERLAP

    # If still None, let exporter pull full history unless user gave a since; recommend setting one for inactive channels
    since_arg = []
    if since_dt:
        since_arg = ["--after", iso(since_dt)]

    until_arg = []
    if until_dt:
        until_arg = ["--before", iso(until_dt)]

    # ---- Compose output filename
    if args.filename:
        out_path = export_dir / args.filename
    else:
        # channel_<id>__after_<...>__before_<...>.json
        s_label = (since_dt.isoformat().replace(":", "-") if since_dt else "start")
        u_label = until_dt.isoformat().replace(":", "-")
        out_path = export_dir / f"channel_{args.channel}__after_{s_label}__before_{u_label}.json"

    # ---- Run exporter
    export_cmd = [
        str(exporter),
        "export",
        "-c", args.channel,
        "-f", "Json",
        "-o", str(out_path),
        "--bot", args.token,
    ] + since_arg + until_arg

    print("[info] Exporting:", " ".join(export_cmd))
    r = subprocess.run(export_cmd, capture_output=True, text=True)
    if r.returncode != 0:
        print(r.stdout)
        print(r.stderr, file=sys.stderr)
        print("[error] Export failed.", file=sys.stderr)
        sys.exit(r.returncode)

    if not out_path.exists() or out_path.stat().st_size == 0:
        print("[info] No messages exported in the window; nothing to forward.")
        sys.exit(0)

    # ---- Find max timestamp in the export
    latest_dt = find_latest_msg_ts_in_export(out_path)
    if latest_dt is None:
        print("[warn] Could not detect latest timestamp in export; forwarding anyway.")

    # ---- Forward via your existing forwarder
    # Choose per-channel state/id-map files so resume/quoting stay isolated
    fwd_state = state_dir / f"forward_state_{args.channel}.json"
    fwd_idmap = state_dir / f"id_map_{args.channel}.json"  # keep if your script supports --id-map

    fwd_cmd = [
        sys.executable,  # python
        "forward_new.py",
        "--webhook", args.webhook,
        "--json", str(out_path),        # <- was --input
        "--state", str(fwd_state),      # <- REQUIRED by your forwarder
        "--max-attach-mb", str(args.max_attach_mb),
        "--max-files-per-post", str(args.max_files_per_post),
    ]

    # Only include if your forward_new.py supports it
    if True:
        fwd_cmd += ["--id-map", str(fwd_idmap)]

    print("[info] Forwarding:", " ".join(fwd_cmd))
    r2 = subprocess.run(fwd_cmd, capture_output=True, text=True)
    print(r2.stdout)
    if r2.returncode != 0:
        print(r2.stderr, file=sys.stderr)
        print("[error] Forwarding failed.", file=sys.stderr)
        sys.exit(r2.returncode)

    # ---- Update state
    if latest_dt:
        save_state(state_dir, args.channel, iso(latest_dt))
        print(f"[info] Updated state last_exported_iso = {iso(latest_dt)}")

    print("[done] Exported and forwarded once.")

if __name__ == "__main__":
    main()
