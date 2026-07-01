import re
from functools import lru_cache
from pathlib import Path

import pytest
## In command line run: uv run pytest -v -rx 2>&1

from bbox_classifier import (
    classify_bbox,
    load_countries,
    _compute_country_areas,
    _COVERAGE_RATIO_THRESHOLD,
    _COVERAGE_FALLBACK_MIN_SHARE,
    _COLLECTIVE_SHARE_THRESHOLD,
    _EUROPEAN_RUSSIA_CLIP_LON,
    _MEANINGFUL_COVERAGE_THRESHOLD,
    _RAW_DOMINANCE_CUTOFF,
    _SINGLE_COUNTRY_SHARE_THRESHOLD,
)

GEOJSON_PATH = "./datadump/world-administrative-boundaries.geojson"

# Loaded once at import so the fixture and the parametrized country test share it.
_EU_GDF, _NON_EU_GDF = load_countries(GEOJSON_PATH)
_COUNTRY_AREAS = _compute_country_areas(_EU_GDF)


@pytest.fixture(scope="module")
def classifier():
    return _EU_GDF, _NON_EU_GDF, _COUNTRY_AREAS


# --- multi_country_eu cases ---
def test_pan_european_bbox_covers_all_eu_countries(classifier):
    # Spans the whole EU set (Iceland to clipped Russia, Cyprus to the Arctic).
    # Asserted against len(eu_gdf) so it tracks the country set, not a magic number.
    eu_gdf, non_eu_gdf, areas = classifier
    result = classify_bbox("[-30, 34, 60, 72]", eu_gdf, non_eu_gdf, areas)
    assert result["classification"] == "multi_country_eu"
    assert "Russian Federation" in result["countries"]
    assert len(result["countries"]) == len(eu_gdf)


def test_atlantic_france_bbox_is_multi_country(classifier):
    eu_gdf, non_eu_gdf, areas = classifier
    result = classify_bbox(
        "[-54.52475, 2.05339, 9.56002, 51.14851]", eu_gdf, non_eu_gdf, areas
    )
    assert result["classification"] == "multi_country_eu"
    assert "France" in result["countries"]
    assert "Spain" in result["countries"]


def test_benelux_bbox_is_multi_country(classifier):
    eu_gdf, non_eu_gdf, areas = classifier
    result = classify_bbox(
        "[2.28516, 49.38237, 7.75635, 53.80065]", eu_gdf, non_eu_gdf, areas
    )
    assert result["classification"] == "multi_country_eu"
    assert "Belgium" in result["countries"]
    assert "Netherlands" in result["countries"]
    assert "Luxembourg" in result["countries"]


# --- single_country_eu cases ---
def test_single_country_bbox_dominant_share_above_half(classifier):
    eu_gdf, non_eu_gdf, areas = classifier
    result = classify_bbox("[6.5, 35.3, 18.6, 47.1]", eu_gdf, non_eu_gdf, areas)
    assert result["classification"] == "single_country_eu"
    assert "Italy" in result["countries"]
    assert result["countries"]["Italy"] > 0.5


def test_luxembourg_bbox_is_single_country_via_coverage(classifier):
    # Luxembourg is fully inside this bbox while Germany/Belgium/France are only
    # clipped, so it neither dominates by raw area (ratio < cutoff) nor holds a
    # majority of the EU intersection (share < 0.50) — rule 1 is skipped and it
    # is resolved single by the coverage-dominance fallback.
    eu_gdf, non_eu_gdf, areas = classifier
    result = classify_bbox("[5.55, 49.42, 6.74, 50.25]", eu_gdf, non_eu_gdf, areas)
    assert result["classification"] == "single_country_eu"
    assert "Luxembourg" in result["countries"]
    assert result["coverage_ratio"] is not None
    assert result["coverage_ratio"] > _COVERAGE_RATIO_THRESHOLD
    assert result["coverage_top_country"] == "Luxembourg"


def test_point_in_paris_is_single_country(classifier):
    eu_gdf, non_eu_gdf, areas = classifier
    result = classify_bbox("[2.35, 48.85, 2.35, 48.85]", eu_gdf, non_eu_gdf, areas)
    assert result["classification"] == "single_country_eu"
    assert "France" in result["countries"]


def test_small_city_bbox_is_single_country(classifier):
    # Ghent city bbox — a small 2D polygon (not a point/line) that fits entirely
    # inside Belgium, so only one EU country is hit and Step 5 resolves it via
    # the single-hit short-circuit rather than dominance/coverage logic.
    eu_gdf, non_eu_gdf, areas = classifier
    result = classify_bbox("[3.57976,50.97954,3.84934,51.18889]", eu_gdf, non_eu_gdf, areas)
    assert result["classification"] == "single_country_eu"
    assert "Belgium" in result["countries"]
    assert result["countries"]["Belgium"] == 1.0


def test_line_within_one_eu_country_is_single_country(classifier):
    # Meridian through Brandenburg/Berlin (lon 13, lat 52-53) — Germany only.
    eu_gdf, non_eu_gdf, areas = classifier
    result = classify_bbox("[13.0, 52.0, 13.0, 53.0]", eu_gdf, non_eu_gdf, areas)
    assert result["classification"] == "single_country_eu"
    assert "Germany" in result["countries"]


def test_line_straddling_eu_non_eu_border_is_single_country(classifier):
    # Accepted misclassification (see ADR 0001, "Accepted limitations"): the
    # line runs Yemen -> Iran -> Kazakhstan -> (Russia); only Russia (10.2% of
    # the length) is EU, so it resolves to single_country_eu with no eu_share
    # guard.
    eu_gdf, non_eu_gdf, areas = classifier
    result = classify_bbox("[54.06, 12.09, 54.06, 54.76]", eu_gdf, non_eu_gdf, areas)
    assert result["classification"] == "single_country_eu"
    assert "Russian Federation" in result["countries"]


# --- multi_country_eu (line) ---
def test_line_crossing_two_eu_countries_is_multi_country(classifier):
    # Parallel at lat 48.5 from lon 2 (France) to lon 13 (Germany) — two EU
    # countries of comparable span, raw length ratio < _RAW_DOMINANCE_CUTOFF -> multi.
    eu_gdf, non_eu_gdf, areas = classifier
    result = classify_bbox("[2.0, 48.5, 13.0, 48.5]", eu_gdf, non_eu_gdf, areas)
    assert result["classification"] == "multi_country_eu"
    assert "France" in result["countries"]
    assert "Germany" in result["countries"]


# --- non_european cases ---
## Other continents
def test_africa_bbox_is_non_european(classifier):
    eu_gdf, non_eu_gdf, areas = classifier
    result = classify_bbox("[-20, -40, 55, 37]", eu_gdf, non_eu_gdf, areas)
    assert result["classification"] == "non_european"


def test_asia_bbox_is_non_european(classifier):
    eu_gdf, non_eu_gdf, areas = classifier
    result = classify_bbox("[50, 5, 150, 55]", eu_gdf, non_eu_gdf, areas)
    assert result["classification"] == "non_european"


def test_south_america_bbox_is_non_european(classifier):
    eu_gdf, non_eu_gdf, areas = classifier
    result = classify_bbox("[-85, -60, -30, 15]", eu_gdf, non_eu_gdf, areas)
    assert result["classification"] == "non_european"

## Neighboring countries
def test_syria_bbox_is_non_european(classifier):
    eu_gdf, non_eu_gdf, areas = classifier
    result = classify_bbox("[35.6145, 32.3136, 42.3783, 37.2905]", eu_gdf, non_eu_gdf, areas)
    assert result["classification"] == "non_european"


def test_iraq_bbox_is_non_european(classifier):
    eu_gdf, non_eu_gdf, areas = classifier
    result = classify_bbox("[38.7947, 29.0617, 48.5607, 37.3837]", eu_gdf, non_eu_gdf, areas)
    assert result["classification"] == "non_european"


def test_armenia_bbox_is_non_european(classifier):
    eu_gdf, non_eu_gdf, areas = classifier
    result = classify_bbox("[43.4542, 38.8412, 46.6205, 41.2971]", eu_gdf, non_eu_gdf, areas)
    assert result["classification"] == "non_european"


def test_georgia_bbox_is_non_european(classifier):
    eu_gdf, non_eu_gdf, areas = classifier
    result = classify_bbox("[40.0030, 41.0480, 46.7108, 43.5847]", eu_gdf, non_eu_gdf, areas)
    assert result["classification"] == "non_european"


def test_iran_bbox_is_non_european(classifier):
    eu_gdf, non_eu_gdf, areas = classifier
    result = classify_bbox("[44.0350, 25.0760, 63.3303, 39.7792]", eu_gdf, non_eu_gdf, areas)
    assert result["classification"] == "non_european"


def test_kazakhstan_bbox_is_non_european(classifier):
    eu_gdf, non_eu_gdf, areas = classifier
    result = classify_bbox("[46.4992, 40.5944, 87.3482, 55.4426]", eu_gdf, non_eu_gdf, areas)
    assert result["classification"] == "non_european"


def test_point_in_non_eu_country_is_non_european(classifier):
    # A point in Morocco — claimed by no EU country -> 0 EU hits.
    eu_gdf, non_eu_gdf, areas = classifier
    result = classify_bbox("[-6.0, 32.0, -6.0, 32.0]", eu_gdf, non_eu_gdf, areas)
    assert result["classification"] == "non_european"


# --- invalid cases ---
def test_projected_metric_coords_are_invalid(classifier):
    eu_gdf, non_eu_gdf, areas = classifier
    result = classify_bbox("[654360, 7847000, 609000, 7880000]", eu_gdf, non_eu_gdf, areas)
    assert result["classification"] == "invalid"


def test_swapped_axis_east_less_than_west_is_invalid(classifier):
    eu_gdf, non_eu_gdf, areas = classifier
    result = classify_bbox("[6.74, 49.42, 5.55, 50.25]", eu_gdf, non_eu_gdf, areas)
    assert result["classification"] == "invalid"


def test_out_of_range_coords_are_invalid(classifier):
    eu_gdf, non_eu_gdf, areas = classifier
    result = classify_bbox("[-180, -90, 180, 100]", eu_gdf, non_eu_gdf, areas)
    assert result["classification"] == "invalid"


# --- every EU country's own bbox (parametrized) ---
#
# Norway and Croatia are the two cases where it is NOT correct — their bbox is
# so dominated by neighbours that the country is not even the plurality hit, and
# no share/coverage threshold recovers it (structural, not tunable):
#   - Norway's bbox reaches far east (to 31.1°E) and encloses all of Sweden +
#     Finland; Norway is only ~27% of the intersection (Sweden 32%, Finland 28%).
#   - Croatia wraps around Bosnia & Herzegovina (in the EU set under the
#     continent convention), so its bbox is ~as much Bosnia as Croatia
#     (Croatia 36%, Bosnia 33%).
# See ADR 0001 ("Accepted limitations") for the full rationale.
#
# The set below is explicit so the test stays a strict regression guard; the
# xfail reasons are computed live from the result dict (_own_bbox_xfail_reason),
# so `pytest -rx` prints the actual decision path for each.
_OWN_BBOX_IS_MULTI = {
    "Norway",
    "Croatia",
}


def _own_bbox_xfail_reason(bbox_str):
    res = classify_bbox(bbox_str, _EU_GDF, _NON_EU_GDF, _COUNTRY_AREAS)
    top = sorted(res["countries"].items(), key=lambda kv: kv[1], reverse=True)[:3]
    top_str = ", ".join(f"{n} {s:.0%}" for n, s in top)
    cov = res["coverage_ratio"]
    cov_str = f"{cov:.2f} ({res['coverage_top_country']})" if cov is not None else "None"
    return (
        f"{res['classification']} | ratio={res['ratio']}, "
        f"coverage_ratio={cov_str} | top: {top_str}"
    )


def _eu_country_bbox_params():
    params = []
    for _, row in _EU_GDF.iterrows():
        minx, miny, maxx, maxy = row.geometry.bounds
        bbox_str = f"[{minx}, {miny}, {maxx}, {maxy}]"
        name = row["name"]
        marks = (
            (pytest.mark.xfail(strict=True, reason=_own_bbox_xfail_reason(bbox_str)),)
            if name in _OWN_BBOX_IS_MULTI
            else ()
        )
        params.append(pytest.param(name, bbox_str, id=name, marks=marks))
    return params


@pytest.mark.parametrize("country, bbox_str", _eu_country_bbox_params())
def test_each_eu_country_own_bbox_is_single_country(country, bbox_str, classifier):
    eu_gdf, non_eu_gdf, areas = classifier
    result = classify_bbox(bbox_str, eu_gdf, non_eu_gdf, areas)
    # The result dict is appended to the assertion so the decision path (countries
    # + shares, ratio, coverage_ratio) is printed ONLY when this test fails.
    assert result["classification"] == "single_country_eu", (
        f"{country} {bbox_str} -> {result['classification']}: {result}"
    )
    assert country in result["countries"]


# --- doc/code sync: flowchart thresholds must match bbox_classifier.py ---
# The bbox-classifier flowchart (docs/bbox-classifier-flowchart.md) embeds the
# live threshold values as literals at its decision nodes. These tests pin every
# literal to its constant so the chart cannot drift silently: edit the chart
# freely, and if a number falls behind, `uv run pytest` fails loudly. If you
# reword a node so a phrase no longer matches, update the template here.

_FLOWCHART_PATH = Path(__file__).resolve().parent / "docs" / "bbox-classifier-flowchart.md"


@lru_cache(maxsize=1)
def _mermaid_block():
    text = _FLOWCHART_PATH.read_text(encoding="utf-8")
    match = re.search(r"```mermaid\n(.*)```", text, re.DOTALL)
    assert match, f"No ```mermaid block found in {_FLOWCHART_PATH}"
    return match.group(1)


def _pct(x):
    return f"{float(x) * 100:g}%"


def _num(x):
    return f"{float(x):g}"


# (label, module constant, phrase template). {pct} renders a fraction as a
# percent already including the sign (0.50 -> "50%"); {num} renders a bare
# number (2 -> "2", 1.5 -> "1.5").
_FLOWCHART_THRESHOLDS = [
    ("Russia clip longitude", _EUROPEAN_RUSSIA_CLIP_LON, "[0°, {num}°E]"),
    ("meaningful coverage", _MEANINGFUL_COVERAGE_THRESHOLD, "coverage ≥ {pct}"),
    ("collective share", _COLLECTIVE_SHARE_THRESHOLD, "collective share ≥ {pct}"),
    ("raw dominance cutoff", _RAW_DOMINANCE_CUTOFF, "raw_ratio > {num}"),
    ("single-country share (majority gate)", _SINGLE_COUNTRY_SHARE_THRESHOLD, "top_share ≥ {pct}"),
    ("single-country share (Russia override)", _SINGLE_COUNTRY_SHARE_THRESHOLD, "holds < {pct}"),
    ("coverage ratio", _COVERAGE_RATIO_THRESHOLD, "coverage_ratio > {num}"),
    ("coverage fallback min share", _COVERAGE_FALLBACK_MIN_SHARE, "share > {pct}"),
]


@pytest.mark.parametrize("label,const,template", _FLOWCHART_THRESHOLDS)
def test_flowchart_thresholds_match_code(label, const, template):
    expected = template.format(pct=_pct(const), num=_num(const))
    assert expected in _mermaid_block(), (
        f"Flowchart out of sync for '{label}': expected literal '{expected}' "
        f"(from {const!r}), not found in {_FLOWCHART_PATH.name}."
    )
