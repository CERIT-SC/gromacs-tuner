from api.engines.amber.runner import _parse_amber_performance, _parse_amber_progress

SAMPLE_MDOUT = """\
 NSTEP =     5000   TIME(PS) =      10.000  TEMP(K) =   300.12
|      Average timings for last    5000 steps:
|      Elapsed(s) =      64.4 Per Step(ms) =      12.9
|      ns/day =     13.4 seconds/ns =    6436.2
|      Average timings for all steps:
|      Elapsed(s) =      64.4 Per Step(ms) =      12.9
|      ns/day =     13.5 seconds/ns =    6436.2
"""

SAMPLE_MDINFO = """\
|  Master Total CPU time:         64.41 seconds
|
|        Nstep =     5000    Time =      10.000
|      ns/day =     13.4 seconds/ns =    6436.2
"""


def test_parse_performance_returns_last_match() -> None:
    # Last ns/day in mdout is the "all steps" summary — should be 13.5
    result = _parse_amber_performance(SAMPLE_MDOUT)
    assert result == 13.5


def test_parse_performance_returns_zero_on_empty() -> None:
    assert _parse_amber_performance("") == 0.0


def test_parse_performance_returns_zero_on_no_match() -> None:
    assert _parse_amber_performance("no performance data here") == 0.0


def test_parse_progress_returns_step() -> None:
    result = _parse_amber_progress(SAMPLE_MDINFO)
    assert result == 5000


def test_parse_progress_returns_none_on_empty() -> None:
    assert _parse_amber_progress("") is None


def test_parse_progress_returns_none_on_no_match() -> None:
    assert _parse_amber_progress("no steps here") is None
