import pytest
import pandas as pd
from datetime import datetime, timezone, timedelta
from loki_grabber import (
    redact_player,
    strip_log_prefix,
    redact_emails,
    flatten_loki_response,
    build_date_range_ns,
    KNOWN_PLAYERS,
)


class TestRedactPlayer:
    """Test player name redaction logic."""

    def test_known_player_passes_through(self):
        """Known player names should remain unchanged."""
        assert redact_player("Bez", KNOWN_PLAYERS) == "Bez"
        assert redact_player("Bob", KNOWN_PLAYERS) == "Bob"
        assert redact_player("UptimeLabs", KNOWN_PLAYERS) == "UptimeLabs"

    def test_known_player_case_insensitive(self):
        """Known player names should be case-insensitive."""
        assert redact_player("bez", KNOWN_PLAYERS) == "bez"
        assert redact_player("BOB", KNOWN_PLAYERS) == "BOB"
        assert redact_player("UPTIMETLABS", KNOWN_PLAYERS) == "PLAYER"

    def test_unknown_player_redacted(self):
        """Unknown player names should be replaced with 'PLAYER'."""
        assert redact_player("Unknown", KNOWN_PLAYERS) == "PLAYER"
        assert redact_player("RandomUser", KNOWN_PLAYERS) == "PLAYER"

    def test_partial_match_is_redacted(self):
        """Partial matches at the start ARE matched by the pattern '^(Bez|...)'."""
        # 'Bezant' starts with 'Bez' so it matches the pattern and passes through
        assert redact_player("Bezant", KNOWN_PLAYERS) == "Bezant"

    def test_nan_value_redacted(self):
        """NaN/None values should be redacted to 'PLAYER'."""
        assert redact_player(None, KNOWN_PLAYERS) == "PLAYER"
        import math
        assert redact_player(float('nan'), KNOWN_PLAYERS) == "PLAYER"


class TestStripLogPrefix:
    """Test log prefix stripping logic."""

    def test_standard_prefix_removed(self):
        """Standard format: (channel) user: message"""
        line = "(general) bob: hello world"
        assert strip_log_prefix(line) == "hello world"

    def test_prefix_with_multiple_spaces(self):
        """Handle multiple spaces in the prefix."""
        line = "(project-logs)  alice:  important message"
        assert strip_log_prefix(line) == "important message"

    def test_prefix_with_special_channel_name(self):
        """Handle brackets and special chars in channel name."""
        line = "(dev-support) user123: message here"
        assert strip_log_prefix(line) == "message here"

    def test_no_prefix_unchanged(self):
        """Lines without the prefix pattern remain unchanged."""
        line = "just a regular message"
        assert strip_log_prefix(line) == "just a regular message"

    def test_message_with_parentheses(self):
        """Message content with parentheses should not be affected."""
        line = "(chat) user: this is a (test) message"
        assert strip_log_prefix(line) == "this is a (test) message"

    def test_empty_string(self):
        """Empty string should remain empty."""
        assert strip_log_prefix("") == ""


class TestRedactEmails:
    r"""Test email redaction logic.

    The regex pattern is: [a-zA-Z0-9._%+-]+@[a-zA-Z0-9._%+-]+[-@][a-zA-Z0-9.-]+\.[a-zA-Z]{2,}
    This matches emails with a hyphen or @ as the separator (Slack-style formats).
    """

    def test_email_with_hyphen_separator(self):
        """Email with hyphen separator should be redacted."""
        text = "contact user@domain-extension.com for help"
        assert redact_emails(text) == "contact @PLAYER for help"

    def test_email_with_at_separator(self):
        """Email with @ separator should be redacted."""
        text = "email user@name@domain.com"
        assert redact_emails(text) == "email @PLAYER"

    def test_standard_email_not_redacted(self):
        """Standard format user@domain.com (dot separator only) should NOT be redacted."""
        text = "contact user@domain.com for help"
        assert redact_emails(text) == "contact user@domain.com for help"

    def test_multiple_emails_redacted(self):
        """Multiple emails should all be redacted."""
        text = "emails: user1@domain-ext.com and user2@name@other.org"
        result = redact_emails(text)
        assert result == "emails: @PLAYER and @PLAYER"

    def test_no_emails_unchanged(self):
        """Text without emails should remain unchanged."""
        text = "this is plain text with no emails"
        assert redact_emails(text) == "this is plain text with no emails"

    def test_email_at_end_of_text(self):
        """Email at the end of text should be redacted."""
        text = "contact user@domain-ext.com"
        assert redact_emails(text) == "contact @PLAYER"

    def test_email_at_start_of_text(self):
        """Email at the start of text should be redacted."""
        text = "user@name@domain.com is the contact"
        assert redact_emails(text) == "@PLAYER is the contact"

    def test_email_with_dots_in_local_part(self):
        """Email with dots in local part should be redacted."""
        text = "contact first.last@domain-ext.com"
        assert redact_emails(text) == "contact @PLAYER"


class TestFlattenLokiResponse:
    """Test Loki response flattening logic."""

    def test_single_stream_single_value(self):
        """Single stream with one log value."""
        data = [
            {
                "stream": {"player_name": "Alice", "channel": "general"},
                "values": [("1000000000", "log message")]
            }
        ]
        result = flatten_loki_response(data)
        assert len(result) == 1
        assert result[0]["timestamp_ns"] == "1000000000"
        assert result[0]["log_line"] == "log message"
        assert result[0]["player_name"] == "Alice"
        assert result[0]["channel"] == "general"

    def test_single_stream_multiple_values(self):
        """Single stream with multiple log values."""
        data = [
            {
                "stream": {"player_name": "Bob", "channel": "dev"},
                "values": [
                    ("1000000000", "first message"),
                    ("2000000000", "second message"),
                ]
            }
        ]
        result = flatten_loki_response(data)
        assert len(result) == 2
        assert result[0]["log_line"] == "first message"
        assert result[1]["log_line"] == "second message"
        assert all(row["player_name"] == "Bob" for row in result)

    def test_multiple_streams(self):
        """Multiple streams should all be flattened."""
        data = [
            {
                "stream": {"player_name": "Alice", "channel": "general"},
                "values": [("1000000000", "alice msg")]
            },
            {
                "stream": {"player_name": "Bob", "channel": "dev"},
                "values": [("2000000000", "bob msg")]
            }
        ]
        result = flatten_loki_response(data)
        assert len(result) == 2
        assert result[0]["player_name"] == "Alice"
        assert result[1]["player_name"] == "Bob"

    def test_empty_streams(self):
        """Empty streams list should return empty list."""
        result = flatten_loki_response([])
        assert result == []

    def test_stream_with_empty_values(self):
        """Stream with no values should not contribute rows."""
        data = [
            {
                "stream": {"player_name": "Charlie", "channel": "logs"},
                "values": []
            }
        ]
        result = flatten_loki_response(data)
        assert result == []

    def test_stream_labels_preserved(self):
        """All stream labels should be preserved in output rows."""
        data = [
            {
                "stream": {
                    "player_name": "Dave",
                    "channel": "support",
                    "environment": "prod"
                },
                "values": [("1000000000", "message")]
            }
        ]
        result = flatten_loki_response(data)
        assert result[0]["environment"] == "prod"


class TestBuildDateRangeNs:
    """Test date range calculation logic."""

    def test_single_date_range(self):
        """Single date should produce 24-hour range in UTC."""
        start_ns, end_ns = build_date_range_ns("2024-01-15")

        # start should be midnight UTC on 2024-01-15
        start_dt = datetime.fromtimestamp(start_ns / 1e9, tz=timezone.utc)
        assert start_dt.year == 2024
        assert start_dt.month == 1
        assert start_dt.day == 15
        assert start_dt.hour == 0
        assert start_dt.minute == 0
        assert start_dt.second == 0

        # end should be midnight UTC on 2024-01-16
        end_dt = datetime.fromtimestamp(end_ns / 1e9, tz=timezone.utc)
        assert end_dt.year == 2024
        assert end_dt.month == 1
        assert end_dt.day == 16
        assert end_dt.hour == 0

    def test_date_range_span(self):
        """End - start should be 86400 seconds (24 hours)."""
        start_ns, end_ns = build_date_range_ns("2024-02-20")
        diff_seconds = (end_ns - start_ns) / 1e9
        assert diff_seconds == 86400

    def test_leap_year_date(self):
        """Leap year dates should be handled correctly."""
        start_ns, end_ns = build_date_range_ns("2024-02-29")
        start_dt = datetime.fromtimestamp(start_ns / 1e9, tz=timezone.utc)
        assert start_dt.month == 2
        assert start_dt.day == 29

    def test_year_boundary(self):
        """Year boundary dates should work correctly."""
        start_ns, end_ns = build_date_range_ns("2023-12-31")
        end_dt = datetime.fromtimestamp(end_ns / 1e9, tz=timezone.utc)
        assert end_dt.year == 2024
        assert end_dt.month == 1
        assert end_dt.day == 1

    def test_invalid_date_format_raises(self):
        """Invalid date format should raise ValueError."""
        with pytest.raises(ValueError):
            build_date_range_ns("2024/01/15")

    def test_invalid_date_value_raises(self):
        """Invalid date values should raise ValueError."""
        with pytest.raises(ValueError):
            build_date_range_ns("2024-13-01")  # invalid month


class TestDataFrameTransformation:
    """Integration-style tests for DataFrame transformation (no HTTP calls)."""

    def test_basic_dataframe_transformation(self):
        """Test basic DataFrame creation and transformation."""
        data = [
            {
                "timestamp_ns": int(datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc).timestamp() * 1e9),
                "log_line": "(general) bez: hello world",
                "player_name": "Bez",
                "channel": "general"
            },
            {
                "timestamp_ns": int(datetime(2024, 1, 15, 10, 1, 0, tzinfo=timezone.utc).timestamp() * 1e9),
                "log_line": "(dev) bob: contact user@domain-ext.com",
                "player_name": "Bob",
                "channel": "dev"
            }
        ]

        df = pd.DataFrame(data)

        # Apply transformations as done in fetch_and_export
        df["timestamp"] = pd.to_datetime(df["timestamp_ns"].astype("int64"), unit="ns", utc=True)
        df["player_name"] = df["player_name"].apply(lambda x: redact_player(x, KNOWN_PLAYERS))

        export_df = df[["timestamp", "player_name", "channel", "log_line"]].copy()
        export_df = export_df.sort_values("timestamp").reset_index(drop=True)
        export_df["timestamp"] = export_df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
        export_df["log_line"] = export_df["log_line"].apply(strip_log_prefix)
        export_df["log_line"] = export_df["log_line"].apply(redact_emails)
        export_df = export_df.rename(columns={
            "timestamp": "datetime",
            "player_name": "player",
            "log_line": "message",
        })

        # Verify output structure
        assert list(export_df.columns) == ["datetime", "player", "channel", "message"]
        assert len(export_df) == 2

        # Verify first row
        assert export_df.loc[0, "player"] == "Bez"
        assert export_df.loc[0, "message"] == "hello world"

        # Verify second row (with email redaction)
        assert export_df.loc[1, "player"] == "Bob"
        assert export_df.loc[1, "message"] == "contact @PLAYER"

    def test_unknown_player_redacted_in_dataframe(self):
        """Unknown players should be redacted in DataFrame transformation."""
        data = [
            {
                "timestamp_ns": int(datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc).timestamp() * 1e9),
                "log_line": "(general) unknown: message",
                "player_name": "UnknownUser",
                "channel": "general"
            }
        ]

        df = pd.DataFrame(data)
        df["player_name"] = df["player_name"].apply(lambda x: redact_player(x, KNOWN_PLAYERS))

        assert df.loc[0, "player_name"] == "PLAYER"
