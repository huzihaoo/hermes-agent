#!/usr/bin/env python3
"""Common Feishu release-target guard helpers for PNC-Agent release tooling."""
from __future__ import annotations

EXPECTED_WIKI_NODE = "DWcXwxUwIiJoIAkgSbFclfcfnLd"
EXPECTED_SPACE_ID = "7558826224870490114"
EXPECTED_WIKI_URL = "https://minieye.feishu.cn/wiki/DWcXwxUwIiJoIAkgSbFclfcfnLd"
OLD_WIKI_NODE = "Wp5awZTinieUjTkyNaYcxAWenpe"


def ensure_release_target(parent_node_token: str, target_id: str) -> None:
    if parent_node_token != EXPECTED_WIKI_NODE:
        raise ValueError(
            f"Refusing release doc target parentNodeToken={parent_node_token!r}; "
            f"expected {EXPECTED_WIKI_NODE!r}. Historical node {OLD_WIKI_NODE!r} is no longer allowed."
        )
    if target_id != EXPECTED_SPACE_ID:
        raise ValueError(
            f"Refusing release doc target targetId={target_id!r}; expected {EXPECTED_SPACE_ID!r}."
        )
