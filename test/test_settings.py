from __future__ import annotations


def test_statistics_table_status_type_removed() -> None:
    """statistics_table_status_type was removed in v0.27.0."""
    from pgqueuer.domain.settings import DBSettings

    assert "statistics_table_status_type" not in DBSettings.model_fields
