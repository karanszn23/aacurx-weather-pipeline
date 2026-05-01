import pytest
from geocode_cities import _hit_rank, _load_geocode_overrides, _select_best_hit


# ---------------------------------------------------------------------------
# _hit_rank
# ---------------------------------------------------------------------------

def _make_hit(country_code="GB", population=100_000):
    return {"country_code": country_code, "population": population}


def test_hit_rank_gb_beats_us():
    gb = _make_hit("GB", 50_000)
    us = _make_hit("US", 500_000)
    assert _hit_rank(gb) < _hit_rank(us)


def test_hit_rank_crown_dependency_beats_non_uk():
    gg = _make_hit("GG", 1_000)    # Guernsey — crown dependency
    fr = _make_hit("FR", 1_000_000)
    assert _hit_rank(gg) < _hit_rank(fr)


def test_hit_rank_larger_population_wins_within_same_country():
    big = _make_hit("GB", 500_000)
    small = _make_hit("GB", 1_000)
    assert _hit_rank(big) < _hit_rank(small)


def test_hit_rank_missing_population_treated_as_zero():
    hit = {"country_code": "GB"}
    # should not raise; population defaults to 0
    rank = _hit_rank(hit)
    assert isinstance(rank, tuple)


def test_hit_rank_none_country_code_does_not_raise():
    hit = {"country_code": None, "population": 0}
    rank = _hit_rank(hit)
    assert isinstance(rank, tuple)


# ---------------------------------------------------------------------------
# _select_best_hit
# ---------------------------------------------------------------------------

def test_select_best_hit_empty_returns_none():
    assert _select_best_hit([]) is None


def test_select_best_hit_single_returns_it():
    hit = _make_hit("GB", 1000)
    result = _select_best_hit([hit])
    assert result is hit


def test_select_best_hit_prefers_gb_over_us():
    gb = _make_hit("GB", 1_000)
    us = _make_hit("US", 10_000_000)
    result = _select_best_hit([us, gb])
    assert result is gb


def test_select_best_hit_prefers_larger_population_within_gb():
    big = {"country_code": "GB", "population": 500_000, "latitude": 51.5, "longitude": -0.1, "name": "London"}
    small = {"country_code": "GB", "population": 5_000, "latitude": 52.0, "longitude": -1.0, "name": "Smalltown"}
    result = _select_best_hit([small, big])
    assert result is big


def test_select_best_hit_prefers_je_over_non_uk():
    je = _make_hit("JE", 500)      # Jersey
    au = _make_hit("AU", 2_000_000)
    result = _select_best_hit([au, je])
    assert result is je


# ---------------------------------------------------------------------------
# _load_geocode_overrides
# ---------------------------------------------------------------------------

def test_load_geocode_overrides_missing_file_returns_empty(tmp_path):
    assert _load_geocode_overrides(str(tmp_path / "missing.csv")) == {}


def test_load_geocode_overrides_reads_manual_city_match(tmp_path):
    overrides_path = tmp_path / "overrides.csv"
    overrides_path.write_text(
        "city,latitude,longitude,geocode_name,admin1,country,country_code,population\n"
        "Springfield,53.1,-1.2,Springfield,England,United Kingdom,GB,12345\n",
        encoding="utf-8",
    )

    overrides = _load_geocode_overrides(str(overrides_path))

    assert overrides["Springfield"]["latitude"] == 53.1
    assert overrides["Springfield"]["longitude"] == -1.2
    assert overrides["Springfield"]["country_code"] == "GB"


def test_load_geocode_overrides_requires_city_lat_lon(tmp_path):
    overrides_path = tmp_path / "bad.csv"
    overrides_path.write_text("city,latitude\nLeeds,53.8\n", encoding="utf-8")

    with pytest.raises(ValueError, match="missing required columns"):
        _load_geocode_overrides(str(overrides_path))
