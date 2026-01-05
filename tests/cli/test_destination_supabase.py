"""Tests for destination supabase command.

NOTE: These tests are skipped by default because:
1. They require Python 3.13+ for native | union type syntax
2. They require live Supabase instances for meaningful integration testing

To test manually:
1. Ensure you're running Python 3.13+
2. Set up a test Supabase project with sample data
3. Update test credentials and remove pytestmark skip decorator
"""

import pytest

# Skip all tests - Supabase CLI requires live instance and Python 3.13+
pytestmark = pytest.mark.skip(reason="Requires Python 3.13+ and live Supabase instance")


def test_destination_supabase_placeholder() -> None:
    """Placeholder test to maintain test structure."""
    # This module exists to document the Supabase destination CLI
    # Manual testing required with actual Supabase credentials
    pass
