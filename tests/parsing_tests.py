import unittest

from datapoint_etl.weather_etl import transform_observations_data


class ParsingTests(unittest.TestCase):
    def test_load_valid_data(self):
        transform_observations_data('valid_data.json', '/dev/null')

    def test_load_broken_data(self):
        transform_observations_data('broken_data.json', '/dev/null')


if __name__ == '__main__':
    unittest.main()
