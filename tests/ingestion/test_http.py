import time
from gold_pipeline.ingestion.http import with_retry, rate_limited

def test_with_retry_eventually_succeeds():
    calls = {"n": 0}

    @with_retry(max_attempts=3)
    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise ConnectionError("boom")
        return "ok"

    assert flaky() == "ok"
    assert calls["n"] == 3

def test_with_retry_reraises_after_exhaustion():
    @with_retry(max_attempts=2)
    def always_fail():
        raise ConnectionError("nope")

    try:
        always_fail()
        assert False, "should have raised"
    except ConnectionError:
        pass

def test_rate_limited_enforces_gap():
    @rate_limited(min_interval_s=0.2)
    def quick():
        return time.monotonic()

    t1 = quick()
    t2 = quick()
    assert t2 - t1 >= 0.2
