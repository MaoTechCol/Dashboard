from __future__ import annotations

import unittest

from app.services.company_registry import normalize_plate_label


class PlateNormalizationTests(unittest.TestCase):
    def test_preserves_valid_colombian_plate(self) -> None:
        self.assertEqual(normalize_plate_label("GHO095"), "GHO095")

    def test_converts_zero_in_letter_prefix_to_o(self) -> None:
        self.assertEqual(normalize_plate_label("GH0095"), "GHO095")

    def test_normalizes_spacing_and_case(self) -> None:
        self.assertEqual(normalize_plate_label(" gh-0095 "), "GHO095")

    def test_leaves_non_colombian_shape_untouched(self) -> None:
        self.assertEqual(normalize_plate_label("48919711"), "48919711")


if __name__ == "__main__":
    unittest.main()
