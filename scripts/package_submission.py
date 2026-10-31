"""Package source, acceptance evidence and the current chat's visible transcript."""
import argparse
import datetime
import hashlib
import json
import os
import pathlib
import time
import zipfile

ROOT = pathlib.Path(__file__).resolve().parents[1]
ARCHIVE = ROOT / "AI-conversation.zip"
PUBLIC_TYPES = {"function_call", "function_call_output", "custom_tool_call", "custom_tool_call_output"}


def records(source):
    data = source.read_bytes()
    result = []
    lines = data.splitlines(keepends=True)
    for index, line in enumerate(lines):
        try:
            result.append((line, json.loads(line)))
        except json.JSONDecodeError:
            if index != len(lines) - 1:
                raise
            # A running session may be in the middle of appending its last row.
    return data, result


def is_public(record):
    if record.get("type") != "response_item":
        return False
    payload = record.get("payload", {})
    if payload.get("type") in PUBLIC_TYPES:
        return True
    return (payload.get("type") == "message" and payload.get("role") in {"user", "assistant"}
            and payload.get("channel") != "analysis" and payload.get("phase") != "analysis")


def write_zip(path, entries):
    temporary = path.with_suffix(path.suffix + ".tmp")
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(temporary, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, content in entries:
            archive.writestr(name, content)
    os.replace(temporary, path)


def export_conversation(source):
    raw, rows = records(source)
    selected = [(line, record) for line, record in rows if is_public(record)]
    if not selected:
        raise ValueError("source has no public conversation records")
    public = b"".join(line if line.endswith(b"\n") else line + b"\n" for line, _ in selected)
    transcript = ["# AI conversation\n", "Public message text and tool events; original selected JSONL records are also included.\n"]
    for _, record in selected:
        item = record["payload"]
        if item["type"] == "message":
            transcript.append("\n## " + item["role"] + "\n")
            content = item.get("content", [])
            if isinstance(content, str):
                transcript.append(content + "\n")
            else:
                for block in content:
                    transcript.append(block.get("text", json.dumps(block, ensure_ascii=False)) + "\n")
        else:
            transcript.append("\n## " + item["type"] + "\n\n````json\n" +
                              json.dumps(item, ensure_ascii=False, indent=2) + "\n````\n")
    manifest = {
        "exported_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "source_session": str(source), "source_sha256": hashlib.sha256(raw).hexdigest(),
        "public_records": len(selected), "public_sha256": hashlib.sha256(public).hexdigest(),
        "scope": "All recorded user/assistant public messages and tool calls/results, with selected JSONL records retained verbatim. System/developer instructions, private reasoning and runtime metadata are excluded.",
        "cutoff": rows[-1][1].get("timestamp") if rows else None,
    }
    write_zip(ARCHIVE, [("conversation.jsonl", public), ("conversation.md", "\n".join(transcript)),
                        ("manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2) + "\n")])
    return rows, manifest


def package(source):
    rows, manifest = export_conversation(source)
    files = []
    for name in ["README.md", "schema.sql", "go.mod", "go.sum", "Dockerfile", "compose.yaml",
                 ".gitignore", ".dockerignore", "AI-conversation.zip"]:
        files.append(ROOT / name)
    for name in ["cmd", "internal", "tests", "migrations", "scripts", ".github", "docs", "artifacts"]:
        for path in sorted((ROOT / name).rglob("*")):
            if path.is_file() and "__pycache__" not in path.parts and path.suffix not in {".exe", ".pyc", ".tmp"}:
                files.append(path)
    entries = [(path.relative_to(ROOT).as_posix(), path.read_bytes()) for path in files]
    entries.append(("SUBMISSION.json", json.dumps({"exported_at": manifest["exported_at"],
        "result": "source and acceptance evidence package", "files": len(files),
        "conversation_records": manifest["public_records"]}, ensure_ascii=False, indent=2)))
    target = ROOT / "submission" / "notification-service.zip"
    write_zip(target, entries)
    print(f"Packaged {len(files)} files; {manifest['public_records']} public conversation records", flush=True)
    return rows


def resolve_source(args):
    if args.session_file:
        return args.session_file.resolve(strict=True)
    thread = os.environ.get("CODEX_THREAD_ID")
    if thread:
        matches = list((pathlib.Path.home() / ".codex" / "sessions").rglob("*" + thread + ".jsonl"))
        if len(matches) == 1:
            return matches[0]
    if ARCHIVE.exists():
        with zipfile.ZipFile(ARCHIVE) as archive:
            return pathlib.Path(json.loads(archive.read("manifest.json"))["source_session"]).resolve(strict=True)
    raise ValueError("provide --session-file for this chat's original JSONL file")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session-file", type=pathlib.Path)
    parser.add_argument("--wait-for-turn-complete", type=int, default=0,
                        help="refresh the package once the currently running chat turn finishes")
    args = parser.parse_args()
    source = resolve_source(args)
    rows = package(source)
    started = [r["payload"].get("turn_id") for _, r in rows
               if r.get("type") == "event_msg" and r.get("payload", {}).get("type") == "task_started"]
    target_turn = started[-1] if started else None
    deadline = time.monotonic() + args.wait_for_turn_complete
    while target_turn and time.monotonic() < deadline:
        _, rows = records(source)
        if any(r.get("type") == "event_msg" and r.get("payload", {}).get("type") == "task_complete"
               and r["payload"].get("turn_id") == target_turn for _, r in rows):
            package(source)
            print("Final assistant reply included; export complete", flush=True)
            return
        time.sleep(1)


if __name__ == "__main__":
    main()
