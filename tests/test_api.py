"""Explorer API: eval formatting (pure) + gated endpoint tests on live graph."""

from __future__ import annotations

import chess
import pytest
from fastapi.testclient import TestClient

from chess_graph.api.app import create_app, format_eval

# --- pure ------------------------------------------------------------------


def test_format_eval_material():
    assert format_eval(31) == "+0.31"
    assert format_eval(-205) == "-2.05"
    assert format_eval(0) == "+0.00"
    assert format_eval(None) is None


def test_format_eval_mate():
    assert format_eval(100000 - 3) == "M3"
    assert format_eval(-(100000 - 3)) == "-M3"


# --- gated integration -----------------------------------------------------


@pytest.fixture
def client():
    app = create_app()
    c = TestClient(app, raise_server_exceptions=True)
    try:
        r = c.get("/health")
        if r.status_code != 200 or r.json().get("positions", 0) == 0:
            pytest.skip("chess_core not loaded")
    except Exception as exc:  # pragma: no cover - env dependent
        pytest.skip(f"FalkorDB not reachable: {exc}")
    return c


def _fen(sans: list[str]) -> str:
    board = chess.Board()
    for san in sans:
        board.push_san(san)
    return board.fen()


def test_explorer_start_position(client):
    r = client.get("/explorer", params={"fen": chess.Board().fen()})
    assert r.status_code == 200
    body = r.json()
    assert body["position"]["min_ply"] == 0
    assert body["position"]["games"] >= 900_000
    sans = {m["san"] for m in body["moves"]}
    assert "e4" in sans and "d4" in sans
    assert r.headers["cache-control"].startswith("public")


def test_explorer_sicilian_practical_vs_engine(client):
    r = client.get("/explorer", params={"fen": _fen(["e4", "c5", "Nf3"])})
    assert r.status_code == 200
    body = r.json()
    assert body["position"]["opening"]["name"].startswith("Sicilian")
    moves = {m["san"]: m for m in body["moves"]}
    assert moves["Nc6"]["games"] > moves["d6"]["games"]  # Nc6 most played
    assert moves["d6"]["is_best"] is True                 # but d6 is the engine's move
    # this position is a real transposition: reached via 1.e4 and 1.Nf3
    lines = body["reached_via"]
    assert any(ln["transposition"] for ln in lines)
    assert lines[0]["line"][:3] == ["e4", "c5", "Nf3"]


def test_explorer_404_for_position_not_in_graph(client):
    # a bare K vs K endgame is never in the opening graph
    r = client.get("/explorer", params={"fen": "8/8/8/4k3/8/4K3/8/8 w - - 0 1"})
    assert r.status_code == 404


def test_explorer_400_for_bad_fen(client):
    r = client.get("/explorer", params={"fen": "not a fen"})
    assert r.status_code == 400
