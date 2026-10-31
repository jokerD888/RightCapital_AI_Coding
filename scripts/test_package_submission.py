import hashlib
import json
import pathlib
import tempfile
import unittest
import zipfile

import package_submission as exporter


class ConversationExportTest(unittest.TestCase):
    def test_public_text_and_tool_events_are_preserved_verbatim(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            source = root / "session.jsonl"
            events = [
                {"type": "response_item", "payload": {"type": "message", "role": "developer", "content": "PRIVATE"}},
                {"type": "response_item", "payload": {"type": "reasoning", "content": "PRIVATE"}},
                {"type": "response_item", "payload": {"type": "message", "role": "assistant", "channel": "analysis", "content": "PRIVATE"}},
                {"type": "response_item", "payload": {"type": "message", "role": "user", "content": [{"text": "保留原文\n第二行"}]}},
                {"type": "response_item", "payload": {"type": "message", "role": "assistant", "content": [{"text": "已完成。"}]}},
                {"type": "response_item", "payload": {"type": "custom_tool_call", "input": "example()"}},
                {"type": "response_item", "payload": {"type": "custom_tool_call_output", "output": "PASS"}},
            ]
            lines = [(json.dumps(event, ensure_ascii=False) + "\n").encode() for event in events]
            source.write_bytes(b"".join(lines))
            previous = exporter.ARCHIVE
            exporter.ARCHIVE = root / "conversation.zip"
            try:
                _, manifest = exporter.export_conversation(source)
                with zipfile.ZipFile(exporter.ARCHIVE) as archive:
                    data = archive.read("conversation.jsonl")
                    self.assertEqual(data, b"".join(lines[3:]))
                    self.assertNotIn(b"PRIVATE", b"".join(archive.read(name) for name in archive.namelist()))
                    self.assertEqual(manifest["public_sha256"], hashlib.sha256(data).hexdigest())
                    self.assertEqual(manifest["public_records"], 4)
            finally:
                exporter.ARCHIVE = previous


if __name__ == "__main__":
    unittest.main()
