"""
Tests for the INPE classes
"""
# import os
# import pytest

# from raindownloader.inpeparser import INPE


# class TestINPE:
#     """Tests the INPE class"""

#     @pytest.fixture
#     def fixture_data(self):
#         """Return the test data for the tests"""
#         data = {
#             "test_dir": "../tmp",
#             "test_date": "2023-03-01",
#             "correct_structure": "2023/03",
#             "correct_filename": "MERGE_CPTEC_20230301.grib2",
#         }

#         return data

#     def test_init(self, fixture_data):
#         """Test that INPE object is initialized with the correct root path"""
#         inpe = INPE(fixture_data["test_dir"])
#         assert inpe.root == os.path.normpath(fixture_data["test_dir"])

#     def test_merge_structure(self, fixture_data):
#         """Test that MERGE_structure returns the correct directory structure"""
#         assert (
#             INPE.MERGE_structure(fixture_data["test_date"])
#             == fixture_data["correct_structure"]
#         )

#     def test_merge_filename(self, fixture_data):
#         """Test that MERGE_filename returns the correct filename"""
#         assert (
#             INPE.MERGE_filename(fixture_data["test_date"])
#             == fixture_data["correct_filename"]
#         )
