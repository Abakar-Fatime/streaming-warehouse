from src.generator import generate


def test_generator_is_deterministic():
    a, ma = generate(2000, seed=3)
    b, mb = generate(2000, seed=3)
    assert a == b and ma == mb


def test_manifest_adds_up():
    events, m = generate(5000, seed=11)
    assert m["lines"] == len(events)
    assert m["expected_quarantined"] == sum(m["quarantined_by_reason"].values())
    ids = {e["event_id"] for e in events}
    # every delivered line is either a quarantine record, a unique good event, or a duplicate
    assert m["lines"] == m["expected_valid_unique"] + m["expected_quarantined"] + m["expected_duplicates_delivered"]
    assert len(ids) <= m["lines"]
