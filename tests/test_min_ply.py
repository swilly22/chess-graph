"""BFS min_ply: shortest route from the start, transpositions, orphans."""

from __future__ import annotations

from chess_graph.enrich.min_ply import bfs_min_ply


def test_linear_chain():
    edges = [(0, 1), (1, 2), (2, 3)]
    assert bfs_min_ply(edges, 0) == {0: 0, 1: 1, 2: 2, 3: 3}


def test_transposition_takes_shorter_route():
    # node 9 reachable at ply 2 (0->5->9) and ply 3 (0->1->2->9): min is 2.
    edges = [(0, 1), (1, 2), (2, 9), (0, 5), (5, 9)]
    dist = bfs_min_ply(edges, 0)
    assert dist[9] == 2


def test_orphan_unreachable_excluded():
    # 7 and 8 form a component not reachable from start 0.
    edges = [(0, 1), (1, 2), (7, 8)]
    dist = bfs_min_ply(edges, 0)
    assert 7 not in dist
    assert 8 not in dist
    assert set(dist) == {0, 1, 2}


def test_start_is_zero():
    assert bfs_min_ply([], 42) == {42: 0}


def test_diamond_shortest():
    # 0->1->3 and 0->2->3 ; both length 2
    edges = [(0, 1), (0, 2), (1, 3), (2, 3)]
    assert bfs_min_ply(edges, 0)[3] == 2
