from datetime import UTC, datetime
import unittest

from niru.play_profile import (
    build_play_profile,
    pacific_week_hour_index,
    update_play_profile,
)


class PlayProfileTests(unittest.TestCase):
    def test_build_profile_dedupes_multiple_runs_in_same_week_hour(self) -> None:
        now = datetime(2026, 3, 30, 12, 0, tzinfo=UTC)
        first = datetime(2026, 3, 23, 20, 15, tzinfo=UTC)
        second = datetime(2026, 3, 23, 20, 45, tzinfo=UTC)

        profile = build_play_profile(completed_at_values=[first, second], now=now)
        slot_index = pacific_week_hour_index(first)

        self.assertEqual(profile["play_profile_hour_counts"][slot_index], 1)
        self.assertEqual(profile["play_profile_weeks_observed"], 2)
        self.assertEqual(profile["play_profile_hour_probabilities"][slot_index], 0.5)

    def test_build_profile_counts_weeks_with_no_play(self) -> None:
        first_run = datetime(2026, 3, 2, 20, 15, tzinfo=UTC)
        now = datetime(2026, 3, 30, 12, 0, tzinfo=UTC)

        profile = build_play_profile(completed_at_values=[first_run], now=now)

        self.assertEqual(profile["play_profile_weeks_observed"], 5)

    def test_update_profile_adds_new_week_hour_without_double_counting_old_one(self) -> None:
        now = datetime(2026, 3, 30, 12, 0, tzinfo=UTC)
        existing = build_play_profile(
            completed_at_values=[datetime(2026, 3, 9, 20, 15, tzinfo=UTC)],
            now=now,
        )
        updated = update_play_profile(
            existing_profile=existing,
            completed_at_values=[datetime(2026, 3, 16, 20, 35, tzinfo=UTC)],
            now=now,
        )
        slot_index = pacific_week_hour_index(datetime(2026, 3, 9, 20, 15, tzinfo=UTC))

        self.assertEqual(updated["play_profile_hour_counts"][slot_index], 2)
        self.assertEqual(updated["play_profile_weeks_observed"], 4)
        self.assertEqual(updated["play_profile_hour_probabilities"][slot_index], 0.5)

    def test_build_profile_buckets_hours_in_pacific_time(self) -> None:
        completed_at = datetime(2026, 3, 24, 6, 30, tzinfo=UTC)

        profile = build_play_profile(completed_at_values=[completed_at], now=completed_at)
        slot_index = pacific_week_hour_index(completed_at)

        self.assertEqual(slot_index, 23)
        self.assertEqual(profile["play_profile_hour_counts"][23], 1)


if __name__ == "__main__":
    unittest.main()
