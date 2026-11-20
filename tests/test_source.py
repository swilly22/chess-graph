"""Streaming .zst line source (local-file branch; no network)."""

from __future__ import annotations

import zstandard

from chess_graph.ingest.source import month_url, open_zst_lines


def test_month_url():
    assert month_url("2026-10") == (
        "https://database.lichess.org/standard/lichess_db_standard_rated_2026-10.pgn.zst"
    )


def test_open_zst_lines_reads_local_file(tmp_path):
    content = "line one\nline two\nline three\n"
    path = tmp_path / "sample.txt.zst"
    path.write_bytes(zstandard.ZstdCompressor().compress(content.encode()))

    with open_zst_lines(str(path)) as lines:
        got = list(lines)
    assert "".join(got) == content
    assert [line.rstrip("\n") for line in got] == ["line one", "line two", "line three"]


def test_open_zst_lines_reads_multiframe(tmp_path):
    c = zstandard.ZstdCompressor()
    frames = c.compress(b"frame-a\n") + c.compress(b"frame-b\n")
    path = tmp_path / "multi.zst"
    path.write_bytes(frames)

    with open_zst_lines(str(path)) as lines:
        got = [line.rstrip("\n") for line in lines]
    assert got == ["frame-a", "frame-b"]


def test_open_zst_lines_supports_early_abort(tmp_path):
    # Consuming only the first line and leaving the context must not error.
    big = "".join(f"line {i}\n" for i in range(10_000))
    path = tmp_path / "big.zst"
    path.write_bytes(zstandard.ZstdCompressor().compress(big.encode()))

    with open_zst_lines(str(path)) as lines:
        first = next(lines)
    assert first == "line 0\n"
