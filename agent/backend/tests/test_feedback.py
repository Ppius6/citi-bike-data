from unittest.mock import patch

import pytest

import feedback
from database import SchemaContext


@pytest.fixture(autouse=True)
def clear_candidates():
    feedback._pending.clear()
    yield
    feedback._pending.clear()


def test_only_positive_feedback_saves_and_cannot_be_replayed():
    with (patch('feedback.get_schema_context', return_value=SchemaContext('', 'v2')),
          patch('feedback.save_memory', return_value=True) as save):
        candidate = feedback.create_candidate('How many rides?', 'SELECT count() FROM gold.fact_trips', 'v2')
        assert feedback.submit_feedback(candidate, False) == 'discarded'
        save.assert_not_called()
        candidate = feedback.create_candidate('How many rides?', 'SELECT count() FROM gold.fact_trips', 'v2')
        assert feedback.submit_feedback(candidate, True) == 'saved'
        assert feedback.submit_feedback(candidate, True) == 'expired'
        save.assert_called_once_with('How many rides?', 'SELECT count() FROM gold.fact_trips', 'v2')


def test_expiration_schema_change_and_capacity(monkeypatch):
    now = [0]
    monkeypatch.setattr(feedback, 'monotonic', lambda: now[0])
    monkeypatch.setattr(feedback, 'MAX_CANDIDATES', 2)
    first = feedback.create_candidate('q', 'SELECT 1', 'v1')
    second = feedback.create_candidate('q', 'SELECT 1', 'v1')
    feedback.create_candidate('q', 'SELECT 1', 'v1')
    assert feedback.submit_feedback(first, True) == 'expired'
    with patch('feedback.get_schema_context', return_value=SchemaContext('', 'v2')):
        assert feedback.submit_feedback(second, True) == 'expired'
    now[0] = 901
    assert feedback.submit_feedback(feedback.create_candidate('q', 'SELECT 1', 'v2'), False) == 'discarded'
    assert len(feedback._pending) == 0


def test_unavailable_memory_can_be_retried():
    candidate = feedback.create_candidate('q', 'SELECT 1', 'v2')
    with (patch('feedback.get_schema_context', return_value=SchemaContext('', 'v2')),
          patch('feedback.save_memory', side_effect=[False, True])):
        assert feedback.submit_feedback(candidate, True) == 'unavailable'
        assert feedback.submit_feedback(candidate, True) == 'saved'
