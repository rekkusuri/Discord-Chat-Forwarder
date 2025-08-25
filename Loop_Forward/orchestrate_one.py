import argparse
import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def load_progress(path: Path) -> dict:
    if not path.exists():
        return {"last_before_iso": None}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"last_before_iso": None}

def save_progress(path: Path, data: dict) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
    tmp.replace(path)

def append_log(log_path: Path, line: str) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as f:
        f.write(line.rstrip() + "\n")

def enforce_retention(dir_path: Path, keep: int) -> None:
    files = sorted([p for p in dir_path.glob("*.json") if p.is_file()],
                   key=lambda p: p.stat().st_mtime, reverse=True)
    for old in files[keep:]:
        try:
            old.unlink()
        except Exception:
            pass

def run(cmd: list[str]) -> tuple[int, str, str]:
    # Capture raw bytes, decode safely (Windows console often isn't UTF-8)
    proc = subprocess.run(cmd, capture_output=True)  # text=False
    def _dec(b):
        if b is None:
            return ""
        try:
            return b.decode("utf-8", "replace")
        except Exception:
            try:
                import locale
                return b.decode(locale.getpreferredencoding(False), "replace")
            except Exception:
                return b.decode("latin-1", "replace")
    return proc.returncode, _dec(proc.stdout), _dec(proc.stderr)

def main():
    ap = argparse.ArgumentParser(description="Export+forward one channel with catch-up, logging, and retention.")
    ap.add_argument("--channel-id", required=True)
    ap.add_argument("--webhook", required=True)
    ap.add_argument("--exporter-exe", required=True)
    ap.add_argument("--bot-token", required=True)
    ap.add_argument("--export-root", required=True, help="Root dir; per-channel subfolder is created here")
    ap.add_argument("--state", required=True, help="Path to dedupe state.json (used by forward_new.py)")
    ap.add_argument("--progress", required=True, help="Path to per-channel progress.json")
    ap.add_argument("--log", help="Path to per-channel .log file (default: <export_dir>/channel.log)")
    ap.add_argument("--window-min", type=int, default=33)
    ap.add_argument("--overlap-min", type=int, default=1)
    ap.add_argument("--retention", type=int, default=100, help="Keep latest N JSON exports per channel")
    ap.add_argument("--forwarder-path", default=None, help="Path to forward_new.py (defaults to alongside this script)")
    ap.add_argument("--max-attach-mb", type=float, default=25.0)
    ap.add_argument("--max-files-per-post", type=int, default=10)
    args = ap.parse_args()

    channel_id = args.channel_id
    export_dir = Path(args.export_root) / channel_id
    export_dir.mkdir(parents=True, exist_ok=True)
    progress_path = Path(args.progress)
    log_path = Path(args.log) if args.log else (export_dir / "channel.log")

    # forwarder path
    forwarder_py = Path(args.forwarder_path) if args.forwarder_path else Path(__file__).with_name("forward_new.py")

    # progress & targets
    progress = load_progress(progress_path)
    now = datetime.now(timezone.utc)

    target_before = now - timedelta(minutes=1)          # exclusive upper bound
    window  = timedelta(minutes=args.window_min)        # e.g., 33
    overlap = timedelta(minutes=args.overlap_min)       # e.g., 1

    last_before_iso = progress.get("last_before_iso")
    if last_before_iso:
        try:
            last_before = datetime.fromisoformat(last_before_iso.replace("Z", "+00:00"))
        except Exception:
            last_before = target_before
    else:
        last_before = target_before - (window - overlap)


    window = timedelta(minutes=args.window_min)
    overlap = timedelta(minutes=args.overlap_min)
    windows_processed = 0

    while True:
        after_dt = last_before - overlap
        before_dt = min(after_dt + window, target_before)
        if before_dt <= after_dt:
            break

        after_iso = iso_z(after_dt)
        before_iso = iso_z(before_dt)
        stamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        out_json = export_dir / f"{channel_id}_{stamp}.json"

        append_log(log_path, f"[{iso_z(now)}] START window {after_iso} → {before_iso}")

        # 1) Export
        exp_cmd = [
            args.exporter_exe, "export",
            "-c", channel_id,
            "-f", "Json",
            "-o", str(out_json),
            "--after", after_iso,
            "--before", before_iso,
            "--bot", args.bot_token,
        ]
        rc, out, err = run(exp_cmd)
        append_log(log_path, f"[export rc={rc}] {out.strip() or '(no stdout)'}")
        if err.strip():
            append_log(log_path, f"[export stderr] {err.strip()}")

        if rc != 0:
            append_log(log_path, f"[ABORT] exporter failed; will retry this window next run.")
            break

        # 2) Forward
        id_map_path = str((export_dir / "id_map.json").resolve())  # per-channel map
        fwd_cmd = [
            sys.executable, str(forwarder_py),
            "--webhook", args.webhook,
            "--json", str(out_json),
            "--state", args.state,
            "--id-map", id_map_path,
            "--max-attach-mb", str(args.max_attach_mb),
            "--max-files-per-post", str(args.max_files_per_post),
        ]

        rc2, out2, err2 = run(fwd_cmd)
        append_log(log_path, f"[forward rc={rc2}] {out2.strip() or '(no stdout)'}")
        if err2.strip():
            append_log(log_path, f"[forward stderr] {err2.strip()}")

        if rc2 != 0:
            append_log(log_path, f"[ABORT] forwarder failed; will retry this window next run.")
            break

        # 3) Success → advance & prune
        progress["last_before_iso"] = before_iso
        save_progress(progress_path, progress)
        enforce_retention(export_dir, args.retention)
        append_log(log_path, f"[DONE] window {after_iso} → {before_iso} | progress.last_before={before_iso}")
        windows_processed += 1

        last_before = before_dt
        if last_before >= target_before:
            break

    append_log(log_path, f"[END] processed_windows={windows_processed}, up_to={progress.get('last_before_iso') or 'N/A'}")

if __name__ == "__main__":
    main()
