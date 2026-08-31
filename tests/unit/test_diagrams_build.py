"""Fixture tests for build.py's scene QA and native-conversion invariants."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

BUILD = Path(__file__).parents[2] / "docs" / "diagrams" / "build.py"
spec = importlib.util.spec_from_file_location("diagrams_build", BUILD)
build = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = build
spec.loader.exec_module(build)

DIAGRAM = build.Diagram(
    name="fixture",
    nodes=(
        build.Node(id="a", label="Source box", col=0, row=0),
        build.Node(id="b", label="Target box", col=1, row=0),
    ),
    edges=(build.Edge(source="a", target="b"),),
)


def _scene():
    # Simulate the canvas round-trip: the server materializes arrow points
    # before GET /api/elements returns the scene the converter consumes.
    elements = build._elements(DIAGRAM)
    for element in elements:
        if element.get("type") == "arrow":
            element.setdefault("x", 0)
            element.setdefault("y", 0)
            element.setdefault("points", [[0, 0], [120, 0]])
    return elements


def test_qa_grid_accepts_the_generated_scene():
    build._qa_grid(DIAGRAM, _scene())


def test_qa_grid_rejects_overlapping_boxes():
    elements = _scene()
    boxes = [e for e in elements if e["type"] == "rectangle"]
    boxes[1]["x"] = boxes[0]["x"]
    boxes[1]["y"] = boxes[0]["y"]
    with pytest.raises(build.CanvasError, match="overlap"):
        build._qa_grid(DIAGRAM, elements)


def test_qa_grid_rejects_clipped_labels():
    elements = _scene()
    box = next(e for e in elements if e["type"] == "rectangle")
    box["width"] = 1
    with pytest.raises(build.CanvasError, match="clipped or missing label"):
        build._qa_grid(DIAGRAM, elements)


def test_qa_grid_rejects_edges_to_unknown_nodes():
    diagram = build.Diagram(
        name="fixture",
        nodes=DIAGRAM.nodes,
        edges=(build.Edge(source="a", target="ghost"),),
    )
    with pytest.raises(build.CanvasError, match="unknown node"):
        build._qa_grid(diagram, build._elements(diagram))


def test_rectangles_overlap_respects_the_gap():
    left = {"x": 0, "y": 0, "width": 100, "height": 50}
    beyond_gap = {"x": 100 + build.BOX_GAP, "y": 0, "width": 100, "height": 50}
    inside_gap = {"x": 120, "y": 0, "width": 100, "height": 50}
    assert not build._rectangles_overlap(left, beyond_gap)
    assert build._rectangles_overlap(left, inside_gap)


def _native():
    return build._native_scene_elements(_scene())


def test_native_scene_conversion_passes_its_own_assertions():
    build._assert_native_scene(_native())


def test_native_scene_rejects_missing_text_binding():
    native = _native()
    rect = next(e for e in native if e["type"] == "rectangle")
    rect["boundElements"] = [
        b for b in rect["boundElements"] if b.get("type") != "text"
    ]
    with pytest.raises(build.CanvasError):
        build._assert_native_scene(native)


def test_native_scene_rejects_wrong_container_id():
    native = _native()
    text = next(e for e in native if e["type"] == "text")
    text["containerId"] = "someone-else"
    with pytest.raises(build.CanvasError):
        build._assert_native_scene(native)


def test_native_scene_rejects_retained_skeleton_keys():
    native = _native()
    rect = next(e for e in native if e["type"] == "rectangle")
    rect["label"] = {"text": "skeleton leftovers"}
    with pytest.raises(build.CanvasError):
        build._assert_native_scene(native)


def test_native_scene_rejects_unbound_arrows():
    native = _native()
    arrow = next(e for e in native if e["type"] == "arrow")
    arrow["startBinding"] = None
    with pytest.raises(build.CanvasError):
        build._assert_native_scene(native)
