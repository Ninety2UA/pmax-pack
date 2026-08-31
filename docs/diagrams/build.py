#!/usr/bin/env python3
"""Build the README diagrams through the Excalidraw canvas REST API.

The canvas frontend performs the image export. Its SVG exporter normally
embeds a glyph-subsetted WOFF2 as a data URL. We verify that property on every
SVG. If a canvas version omits it, set PMAX_EXCALIFONT_SOURCE to an Excalifont
WOFF2 and ensure pyftsubset is on PATH; the fallback subsets the exact diagram
labels and injects that WOFF2 into the SVG. No external font fetch is allowed.

The existing canvas is saved to a temporary JSON file and restored in a
finally block. Diagram elements are generated from a compact grid: rectangles
first, arrows last, with rectangle width derived from label length.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any


INK = "#18181B"
ACCENT = "#047857"
PALETTE = {
    "source": ("#E7F5FF", "#1971C2"),
    "runtime": ("#D1FAE5", ACCENT),
    "data": ("#F4F4F5", "#52525B"),
    "output": ("#FEF3C7", "#B45309"),
    "control": ("#F3E8FF", "#7E22CE"),
}
GRID_X = 400
GRID_Y = 175
ORIGIN_X = 70
ORIGIN_Y = 70
BOX_MIN_WIDTH = 200
BOX_HEIGHT = 84
BOX_GAP = 42
BASE62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"


class CanvasError(RuntimeError):
    """A canvas request or diagram quality gate failed."""


@dataclass(frozen=True)
class Node:
    id: str
    label: str
    col: int
    row: int
    kind: str = "data"


@dataclass(frozen=True)
class Edge:
    source: str
    target: str
    dashed: bool = False


@dataclass(frozen=True)
class Diagram:
    name: str
    nodes: tuple[Node, ...]
    edges: tuple[Edge, ...]


DIAGRAMS = (
    Diagram(
        "architecture",
        (
            Node("ads-api", "Google Ads API", 0, 1, "source"),
            Node("cloud-run", "One Cloud Run Job\nimage pinned by digest", 1, 1, "runtime"),
            Node("raw", "BigQuery raw\nlanded GAQL families", 2, 1, "data"),
            Node("marts", "Staging + intermediates\nadditive marts + views", 3, 1, "data"),
            Node("report", "Validation report\nand observation backup", 4, 1, "output"),
            Node(
                "scheduler",
                "Cloud Scheduler\n04:00 configured deployment timezone",
                1,
                0,
                "control",
            ),
            Node("secret", "Pinned secret version\n+ private YAML config", 1, 2, "control"),
        ),
        (
            Edge("ads-api", "cloud-run"),
            Edge("cloud-run", "raw"),
            Edge("raw", "marts"),
            Edge("marts", "report"),
            Edge("scheduler", "cloud-run"),
            Edge("secret", "cloud-run"),
        ),
    ),
    Diagram(
        "data-model",
        (
            Node("api", "Google Ads API\nGAQL families A to D", 0, 0, "source"),
            Node("raw", "pmax_raw\nlanded history", 1, 0, "data"),
            Node("staging", "Staging\nlatest row per key", 2, 0, "data"),
            Node("intermediate", "Intermediate\ntyped history +\nprovenance", 3, 0, "data"),
            Node("marts", "Additive marts\nperformance + entities", 4, 0, "runtime"),
            Node("views", "Ratio views\nSUM over SUM", 5, 0, "output"),
            Node("reference", "Pinned pMaximizer\nreference queries", 2, 2, "source"),
            Node(
                "rules",
                "Explicit rules mapping\nnot a dependency of\nthe daily marts",
                3,
                2,
                "control",
            ),
            Node("scores", "Best-practice score marts\ncampaign + asset group", 4, 2, "runtime"),
        ),
        (
            Edge("api", "raw"),
            Edge("raw", "staging"),
            Edge("staging", "intermediate"),
            Edge("intermediate", "marts"),
            Edge("marts", "views"),
            Edge("marts", "scores"),
            Edge("reference", "rules", True),
            Edge("rules", "scores", True),
        ),
    ),
    Diagram(
        "cohort-mechanism",
        (
            Node("lag", "Conversion-lag buckets\ncampaign + asset group", 0, 0, "source"),
            Node("prefix", "Bucket reading\nprefix through day D", 1, 0, "data"),
            Node("bucket-cells", "Measured cohort cells\nby lag boundary", 2, 0, "runtime"),
            Node(
                "observations",
                "Append-only observations\nasset cumulative values",
                0,
                2,
                "source",
            ),
            Node("snapshot", "Snapshot reading\nvalue seen on\nclick date + D", 1, 2, "data"),
            Node(
                "asset-cells",
                "Measured or carried\ncohort cells\nmaximum five-day carry",
                2,
                2,
                "runtime",
            ),
            Node("contract", "Window + provenance\nmaturity +\nobserved-through", 3, 1, "control"),
            Node("cohort-marts", "Additive cohort marts\nfixed click-day cost", 4, 1, "runtime"),
            Node("ratios", "Cohort CPA + ROAS\nratio views", 5, 1, "output"),
        ),
        (
            Edge("lag", "prefix"),
            Edge("prefix", "bucket-cells"),
            Edge("observations", "snapshot"),
            Edge("snapshot", "asset-cells"),
            Edge("bucket-cells", "contract"),
            Edge("asset-cells", "contract"),
            Edge("contract", "cohort-marts"),
            Edge("cohort-marts", "ratios"),
        ),
    ),
    Diagram(
        "daily-run",
        (
            Node(
                "scheduler",
                "Scheduler fires\n04:00 configured\ndeployment timezone",
                0,
                0,
                "control",
            ),
            Node("lease", "Acquire lease\none writer continues", 1, 0, "control"),
            Node("extract", "Extract + load\nallowlisted accounts only", 2, 0, "source"),
            Node("observe", "Append observation log\n+ backup", 3, 0, "output"),
            Node(
                "transform",
                "Checkpointed extraction\n+ window-bound transforms",
                4,
                0,
                "data",
            ),
            Node("validate", "Validate + report\nPASS, FAIL, or SKIPPED", 5, 0, "runtime"),
            Node("digest", "Digest-pinned image\nnumeric secret version", 2, 2, "runtime"),
            Node("first-run", "First run ladder\nScheduler stays paused", 5, 2, "control"),
        ),
        (
            Edge("scheduler", "lease"),
            Edge("lease", "extract"),
            Edge("extract", "observe"),
            Edge("observe", "transform"),
            Edge("transform", "validate"),
            Edge("digest", "extract", True),
            Edge("validate", "first-run", True),
        ),
    ),
)


def _request(
    base_url: str,
    method: str,
    path: str,
    payload: dict[str, Any] | None = None,
    *,
    timeout: int = 45,
) -> dict[str, Any]:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}{path}",
        data=body,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
            if response.status < 200 or response.status >= 300:
                raise CanvasError(f"{method} {path} returned HTTP {response.status}: {raw}")
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        if error.code == 503:
            raise CanvasError(
                f"{method} {path} returned HTTP 503: no browser client is attached: {detail}"
            ) from error
        raise CanvasError(f"{method} {path} returned HTTP {error.code}: {detail}") from error
    except urllib.error.URLError as error:
        raise CanvasError(f"{method} {path} failed: {error.reason}") from error
    try:
        result = json.loads(raw)
    except json.JSONDecodeError as error:
        raise CanvasError(f"{method} {path} returned invalid JSON") from error
    if not isinstance(result, dict) or result.get("success") is False:
        raise CanvasError(f"{method} {path} failed: {result}")
    return result


def _health(base_url: str) -> dict[str, Any]:
    health = _request(base_url, "GET", "/health")
    if "websocket_clients" not in health:
        raise CanvasError("canvas health response omitted websocket_clients")
    clients = health.get("websocket_clients")
    if not isinstance(clients, int) or clients < 0:
        raise CanvasError("canvas health websocket_clients must be a non-negative integer")
    if clients == 0:
        raise CanvasError(
            "canvas has no attached browser client (health websocket_clients must be at least 1)"
        )
    return health


def _label_width(label: str) -> int:
    longest_line = max(len(line) for line in label.splitlines())
    return max(BOX_MIN_WIDTH, int(longest_line * 0.6 * 20 + 48))


def _label_height(label: str) -> int:
    return max(BOX_HEIGHT, len(label.splitlines()) * 28 + 32)


def _box(node: Node) -> dict[str, Any]:
    background, stroke = PALETTE[node.kind]
    return {
        "id": node.id,
        "type": "rectangle",
        "x": ORIGIN_X + node.col * GRID_X,
        "y": ORIGIN_Y + node.row * GRID_Y,
        "width": _label_width(node.label),
        "height": _label_height(node.label),
        "backgroundColor": background,
        "strokeColor": stroke,
        "strokeWidth": 2,
        "roughness": 1,
        "fillStyle": "solid",
        "roundness": {"type": 3},
        "fontFamily": "5",
        "fontSize": 20,
        "label": {"text": node.label},
    }


def _arrow(index: int, edge: Edge) -> dict[str, Any]:
    return {
        "id": f"edge-{index}-{edge.source}-{edge.target}",
        "type": "arrow",
        "x": 0,
        "y": 0,
        "start": {"id": edge.source},
        "end": {"id": edge.target},
        "strokeColor": ACCENT if not edge.dashed else "#71717A",
        "strokeWidth": 2,
        "strokeStyle": "dashed" if edge.dashed else "solid",
        "roughness": 1,
        "endArrowhead": "arrow",
    }


def _elements(diagram: Diagram) -> list[dict[str, Any]]:
    nodes = [_box(node) for node in diagram.nodes]
    arrows = [_arrow(index, edge) for index, edge in enumerate(diagram.edges)]
    return nodes + arrows


def _rectangles_overlap(left: dict[str, Any], right: dict[str, Any]) -> bool:
    return not (
        left["x"] + left["width"] + BOX_GAP <= right["x"]
        or right["x"] + right["width"] + BOX_GAP <= left["x"]
        or left["y"] + left["height"] + BOX_GAP <= right["y"]
        or right["y"] + right["height"] + BOX_GAP <= left["y"]
    )


def _qa_grid(diagram: Diagram, elements: list[dict[str, Any]]) -> None:
    """Scene-based QA used when browser screenshot automation is unavailable."""
    boxes = [element for element in elements if element.get("type") == "rectangle"]
    if len(boxes) != len(diagram.nodes):
        raise CanvasError(
            f"{diagram.name}: expected {len(diagram.nodes)} boxes, found {len(boxes)}"
        )
    for box in boxes:
        label = box.get("label", {}).get("text", "")
        if (
            not label
            or box.get("width", 0) < _label_width(label)
            or box.get("height", 0) < _label_height(label)
        ):
            raise CanvasError(f"{diagram.name}: clipped or missing label in {box.get('id')}")
    for index, left in enumerate(boxes):
        for right in boxes[index + 1 :]:
            if _rectangles_overlap(left, right):
                raise CanvasError(
                    f"{diagram.name}: boxes {left.get('id')} and {right.get('id')} overlap"
                )
    node_ids = {node.id for node in diagram.nodes}
    for edge in diagram.edges:
        if edge.source not in node_ids or edge.target not in node_ids:
            raise CanvasError(f"{diagram.name}: edge refers to an unknown node")


def _stable_int(element_id: str, salt: str) -> int:
    value = zlib.crc32(f"{element_id}:{salt}".encode("utf-8")) & 0x7FFFFFFF
    return value or 1


def _index(position: int) -> str:
    if position >= len(BASE62):
        raise CanvasError("diagram has too many elements for the native index allocator")
    return f"a{BASE62[position]}"


def _native_defaults(
    element: dict[str, Any],
    position: int,
    *,
    bound_elements: list[dict[str, str]] | None,
) -> dict[str, Any]:
    element_id = str(element["id"])
    return {
        **element,
        "angle": 0,
        "opacity": 100,
        "groupIds": [],
        "frameId": None,
        "index": _index(position),
        "seed": _stable_int(element_id, "seed"),
        "version": 1,
        "versionNonce": _stable_int(element_id, "nonce"),
        "isDeleted": False,
        "boundElements": bound_elements,
        "updated": 1,
        "link": None,
        "locked": False,
    }


def _native_scene_elements(scene_elements: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Convert canvas REST skeletons into loadable Excalidraw elements."""
    boxes = [element for element in scene_elements if element.get("type") == "rectangle"]
    arrows = [element for element in scene_elements if element.get("type") == "arrow"]
    box_ids = {str(box.get("id")) for box in boxes}
    text_ids = {str(box["id"]): f"{box['id']}-label" for box in boxes}

    native: list[dict[str, Any]] = []
    for box in boxes:
        box_id = str(box["id"])
        label = box.get("label", {}).get("text")
        if not isinstance(label, str) or not label:
            raise CanvasError(f"rectangle {box_id} has no label")
        references: list[dict[str, str]] = [
            {"id": text_ids[box_id], "type": "text"}
        ]
        for arrow in arrows:
            start_id = arrow.get("start", {}).get("id")
            end_id = arrow.get("end", {}).get("id")
            if box_id in {start_id, end_id}:
                references.append({"id": str(arrow["id"]), "type": "arrow"})
        rectangle = {
            "id": box_id,
            "type": "rectangle",
            "x": box["x"],
            "y": box["y"],
            "width": box["width"],
            "height": box["height"],
            "strokeColor": box.get("strokeColor", INK),
            "backgroundColor": box.get("backgroundColor", "#ffffff"),
            "fillStyle": box.get("fillStyle", "solid"),
            "strokeWidth": box.get("strokeWidth", 2),
            "strokeStyle": box.get("strokeStyle", "solid"),
            "roughness": box.get("roughness", 1),
            "roundness": box.get("roundness") or {"type": 3},
        }
        native.append(
            _native_defaults(rectangle, len(native), bound_elements=references)
        )

    for box in boxes:
        box_id = str(box["id"])
        label = str(box["label"]["text"])
        font_size = int(box.get("fontSize", 20))
        lines = label.splitlines()
        text_width = min(
            float(box["width"]) - 20,
            max(len(line) for line in lines) * 0.6 * font_size,
        )
        text_height = len(lines) * font_size * 1.25
        text = {
            "id": text_ids[box_id],
            "type": "text",
            "x": float(box["x"]) + (float(box["width"]) - text_width) / 2,
            "y": float(box["y"]) + (float(box["height"]) - text_height) / 2,
            "width": text_width,
            "height": text_height,
            "strokeColor": box.get("strokeColor", INK),
            "backgroundColor": "transparent",
            "fillStyle": "solid",
            "strokeWidth": 1,
            "strokeStyle": "solid",
            "roughness": 1,
            "roundness": None,
            "fontSize": font_size,
            "fontFamily": 5,
            "text": label,
            "originalText": label,
            "textAlign": "center",
            "verticalAlign": "middle",
            "containerId": box_id,
            "lineHeight": 1.25,
            "autoResize": True,
        }
        native.append(_native_defaults(text, len(native), bound_elements=None))

    for arrow in arrows:
        arrow_id = str(arrow["id"])
        start_id = arrow.get("start", {}).get("id")
        end_id = arrow.get("end", {}).get("id")
        if start_id not in box_ids or end_id not in box_ids:
            raise CanvasError(f"arrow {arrow_id} has an invalid endpoint")
        points = arrow.get("points")
        if not isinstance(points, list) or len(points) < 2:
            raise CanvasError(f"arrow {arrow_id} has no valid points")
        point_x = [float(point[0]) for point in points]
        point_y = [float(point[1]) for point in points]
        linear = {
            "id": arrow_id,
            "type": "arrow",
            "x": arrow["x"],
            "y": arrow["y"],
            "width": max(point_x) - min(point_x),
            "height": max(point_y) - min(point_y),
            "strokeColor": arrow.get("strokeColor", ACCENT),
            "backgroundColor": "transparent",
            "fillStyle": "solid",
            "strokeWidth": arrow.get("strokeWidth", 2),
            "strokeStyle": arrow.get("strokeStyle", "solid"),
            "roughness": arrow.get("roughness", 1),
            "roundness": None,
            "points": points,
            "startBinding": {
                "elementId": start_id,
                "focus": 0,
                "gap": 1,
                "fixedPoint": None,
            },
            "endBinding": {
                "elementId": end_id,
                "focus": 0,
                "gap": 1,
                "fixedPoint": None,
            },
            "startArrowhead": None,
            "endArrowhead": arrow.get("endArrowhead", "arrow"),
            "lastCommittedPoint": None,
            "elbowed": False,
        }
        native.append(_native_defaults(linear, len(native), bound_elements=None))
    _assert_native_scene(native)
    return native


def _assert_native_scene(elements: list[dict[str, Any]]) -> None:
    by_id = {element.get("id"): element for element in elements}
    rectangles = [element for element in elements if element.get("type") == "rectangle"]
    arrows = [element for element in elements if element.get("type") == "arrow"]
    for rectangle in rectangles:
        text_refs = [
            ref
            for ref in rectangle.get("boundElements") or []
            if ref.get("type") == "text"
        ]
        if len(text_refs) != 1:
            raise CanvasError(
                f"native rectangle {rectangle.get('id')} must bind exactly one text element"
            )
        text = by_id.get(text_refs[0].get("id"))
        if not text or text.get("type") != "text":
            raise CanvasError(f"native rectangle {rectangle.get('id')} binds missing text")
        if text.get("containerId") != rectangle.get("id"):
            raise CanvasError(f"native text for {rectangle.get('id')} has wrong containerId")
        if "label" in rectangle:
            raise CanvasError(f"native rectangle {rectangle.get('id')} retained REST label")
    for arrow in arrows:
        if not arrow.get("startBinding") or not arrow.get("endBinding"):
            raise CanvasError(f"native arrow {arrow.get('id')} lacks bindings")
        if "start" in arrow or "end" in arrow:
            raise CanvasError(f"native arrow {arrow.get('id')} retained REST endpoints")
        for binding in (arrow["startBinding"], arrow["endBinding"]):
            rectangle = by_id.get(binding.get("elementId"))
            references = rectangle.get("boundElements") if rectangle else []
            if not any(
                ref.get("id") == arrow.get("id") and ref.get("type") == "arrow"
                for ref in references or []
            ):
                raise CanvasError(
                    f"native arrow {arrow.get('id')} is not bound back from its rectangle"
                )


def _extract_image(result: dict[str, Any], expected_format: str) -> str:
    if result.get("format") != expected_format or not isinstance(result.get("data"), str):
        raise CanvasError(f"export returned malformed {expected_format} data")
    return result["data"]


def _embedded_woff2(svg: str) -> bool:
    lowered = svg.lower()
    return "@font-face" in lowered and "data:font/woff2;base64," in lowered


def _assert_white_svg_background(svg: str, diagram_name: str) -> None:
    try:
        root = ET.fromstring(svg)
    except ET.ParseError as error:
        raise CanvasError(f"{diagram_name}: SVG export is malformed") from error
    first_rect = next(
        (
            element
            for element in root.iter()
            if element.tag.rsplit("}", 1)[-1] == "rect"
        ),
        None,
    )
    if first_rect is None or first_rect.get("fill", "").lower() != "#ffffff":
        raise CanvasError(
            f"{diagram_name}: SVG export does not begin with a #ffffff background rect"
        )


def _inject_subset(svg: str, labels: str) -> str:
    source = os.environ.get("PMAX_EXCALIFONT_SOURCE", "")
    tool = shutil.which("pyftsubset")
    if not source or not Path(source).is_file() or tool is None:
        raise CanvasError(
            "SVG export omitted its embedded WOFF2 subset; set PMAX_EXCALIFONT_SOURCE "
            "and install pyftsubset for the documented fallback"
        )
    with tempfile.TemporaryDirectory(prefix="pmax-font-subset-") as temp_dir:
        subset = Path(temp_dir) / "excalifont-subset.woff2"
        process = subprocess.run(
            [
                tool,
                source,
                f"--output-file={subset}",
                "--flavor=woff2",
                f"--text={labels}",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        if process.returncode != 0 or not subset.is_file():
            raise CanvasError(f"pyftsubset failed: {process.stderr.strip()}")
        encoded = base64.b64encode(subset.read_bytes()).decode("ascii")
    style = (
        "<defs><style>@font-face{font-family:Excalifont;"
        f"src:url(data:font/woff2;base64,{encoded}) format('woff2');"
        "font-weight:400;font-style:normal}</style></defs>"
    )
    marker = svg.find(">")
    if marker == -1:
        raise CanvasError("cannot inject a WOFF2 subset into malformed SVG")
    return svg[: marker + 1] + style + svg[marker + 1 :]


def _write_diagram(
    base_url: str,
    output_dir: Path,
    diagram: Diagram,
) -> None:
    request_elements = _elements(diagram)
    _qa_grid(diagram, request_elements)
    _request(base_url, "DELETE", "/api/elements/clear")
    _request(base_url, "POST", "/api/elements/batch", {"elements": request_elements})
    _request(base_url, "POST", "/api/viewport", {"scrollToContent": True})
    time.sleep(0.2)

    scene_result = _request(base_url, "GET", "/api/elements")
    scene_elements = scene_result.get("elements")
    if not isinstance(scene_elements, list):
        raise CanvasError(f"{diagram.name}: canvas returned no element list")
    _qa_grid(diagram, scene_elements)
    native_elements = _native_scene_elements(scene_elements)

    envelope = {
        "type": "excalidraw",
        "version": 2,
        "source": "pmax-pack",
        "elements": native_elements,
        "appState": {"viewBackgroundColor": "#ffffff", "exportBackground": True},
        "files": {},
    }
    (output_dir / f"{diagram.name}.excalidraw").write_text(
        json.dumps(envelope, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    _health(base_url)
    svg_result = _request(
        base_url,
        "POST",
        "/api/export/image",
        {"format": "svg", "background": True},
    )
    svg = _extract_image(svg_result, "svg")
    if "<svg" not in svg:
        raise CanvasError(f"{diagram.name}: SVG export contains no svg root")
    _assert_white_svg_background(svg, diagram.name)
    if not _embedded_woff2(svg):
        labels = "".join(node.label for node in diagram.nodes)
        svg = _inject_subset(svg, labels)
    (output_dir / f"{diagram.name}.svg").write_text(svg + "\n", encoding="utf-8")

    _health(base_url)
    png_result = _request(
        base_url,
        "POST",
        "/api/export/image",
        {"format": "png", "background": True},
    )
    try:
        png = base64.b64decode(_extract_image(png_result, "png"), validate=True)
    except (ValueError, base64.binascii.Error) as error:
        raise CanvasError(f"{diagram.name}: PNG export is not valid base64") from error
    if not png.startswith(b"\x89PNG\r\n\x1a\n"):
        raise CanvasError(f"{diagram.name}: PNG export has the wrong signature")
    (output_dir / f"{diagram.name}.png").write_bytes(png)
    # The QA duplicate stays private through scripts/publish-exclude.txt.
    (output_dir / "qa" / f"{diagram.name}.png").write_bytes(png)
    print(f"built {diagram.name}: excalidraw, svg, png, qa/png")


def build(base_url: str, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "qa").mkdir(parents=True, exist_ok=True)
    _health(base_url)
    with tempfile.TemporaryDirectory(prefix="pmax-canvas-snapshot-") as temp_dir:
        snapshot_path = Path(temp_dir) / "canvas-elements.json"
        snapshot = _request(base_url, "GET", "/api/elements")
        snapshot_elements = snapshot.get("elements")
        if not isinstance(snapshot_elements, list):
            raise CanvasError("canvas snapshot contains no element list")
        snapshot_path.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
        try:
            for diagram in DIAGRAMS:
                _write_diagram(base_url, output_dir, diagram)
        finally:
            try:
                saved = json.loads(snapshot_path.read_text(encoding="utf-8"))
                saved_elements = saved.get("elements")
                if not isinstance(saved_elements, list):
                    raise CanvasError("saved canvas snapshot contains no element list")
                _request(base_url, "DELETE", "/api/elements/clear")
                _request(
                    base_url,
                    "POST",
                    "/api/elements/sync",
                    {"elements": saved_elements, "timestamp": int(time.time() * 1000)},
                )
                restored = _request(base_url, "GET", "/api/elements").get("elements")
                if not isinstance(restored, list) or len(restored) != len(saved_elements):
                    raise CanvasError(
                        "canvas restore count mismatch: "
                        f"expected {len(saved_elements)}, got "
                        f"{len(restored) if isinstance(restored, list) else 'invalid'}"
                    )
                print(f"restored canvas snapshot from temporary file ({snapshot_path})")
            except BaseException as restore_error:
                raise CanvasError(f"canvas restore failed: {restore_error}") from restore_error


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--canvas-url",
        default=os.environ.get("EXPRESS_SERVER_URL", "http://localhost:3000"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).resolve().parent,
    )
    args = parser.parse_args()
    try:
        build(args.canvas_url, args.output_dir.resolve())
    except CanvasError as error:
        print(f"build.py: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
