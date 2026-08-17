from __future__ import annotations

import unittest

from app.services.company_registry import CompanyRegistry, is_colombian_plate_label, normalize_plate_label


class PlateNormalizationTests(unittest.TestCase):
    def test_preserves_valid_colombian_plate(self) -> None:
        self.assertEqual(normalize_plate_label("GHO095"), "GHO095")

    def test_converts_zero_in_letter_prefix_to_o(self) -> None:
        self.assertEqual(normalize_plate_label("GH0095"), "GHO095")

    def test_normalizes_spacing_and_case(self) -> None:
        self.assertEqual(normalize_plate_label(" gh-0095 "), "GHO095")

    def test_leaves_non_colombian_shape_untouched(self) -> None:
        self.assertEqual(normalize_plate_label("48919711"), "48919711")

    def test_recognizes_colombian_plate_shape(self) -> None:
        self.assertTrue(is_colombian_plate_label("TTR888"))
        self.assertFalse(is_colombian_plate_label("867869064064439"))

    def test_canonical_plate_prefers_catalog_plate_over_device_identifier(self) -> None:
        registry = CompanyRegistry.__new__(CompanyRegistry)
        self.assertEqual(
            registry.canonical_plate("867869064064439", "TTR888", "867869064064439"),
            "TTR888",
        )


if __name__ == "__main__":
    unittest.main()
