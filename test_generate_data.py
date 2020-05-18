import unittest
import pandas as pd
from generate_data import parse


def assert_csv_equal(test, df, expected):
    test.assertEqual(df.to_csv(index=False).rstrip(), expected)


class TestGenerateData(unittest.TestCase):
    def test_transform_header(self):
        df = parse(('Dagsetning,Smit_Samtals\n'
                    '2020-03-29,156'), 'Smit_Samtals', 'cases')
        assert_csv_equal(self, df, ('date,cases\n' '2020-03-29,156'))

    def test_corrects_datetime_format(self):
        df = parse(('Dagsetning,Smit_Samtals\n'
                    '03-29-2020,156\n'
                    '03-30-2020,186'), 'Smit_Samtals', 'cases')
        assert_csv_equal(self, df, ('date,cases\n'
                                    '2020-03-29,156\n'
                                    '2020-03-30,186'))

    # def test_cumulative_sum(self):
    #     df = parse(('Dagsetning,Smit_Samtals\n'
    #                 '2020-03-29,0\n'
    #                 '2020-03-30,1\n'
    #                 '2020-03-31,0\n'
    #                 '2020-04-01,2\n'
    #                 '2020-04-02,0\n'
    #                 '2020-04-03,0\n'
    #                 '2020-04-04,5'),
    #                'Smit_Samtals',
    #                'cases',
    #                cumsum=True)
    #     assert_csv_equal(
    #         self,
    #         df,
    #         ('date,cases\n'
    #          '2020-03-29,0\n'
    #          '2020-03-30,1\n'
    #          '2020-03-31,1\n'
    #          '2020-04-01,3\n'
    #          '2020-04-02,3\n'
    #          '2020-04-03,3\n'
    #          '2020-04-04,8'),
    #     )

    def test_validation_date_format(self):
        with self.assertRaises(Exception) as cm:
            parse(('Dagsetning,Smit_Samtals\n'
                   ',156'), 'Smit_Samtals', 'cases')
        exceptions = cm.exception.args[0]
        exception = exceptions[0]
        self.assertRegex(str(exceptions[0]),
                         'does not match the date format string "%Y-%m-%d"')

    def test_validation_date_monotonic(self):
        with self.assertRaises(Exception) as cm:
            parse(('Dagsetning,Smit_Samtals\n'
                   '2020-03-29,156\n'
                   '2020-03-28,156'), 'Smit_Samtals', 'cases')
        exceptions = cm.exception.args[0]
        exception = exceptions[0]
        self.assertRegex(str(exceptions[0]), 'date is not monotonic')

    def test_validation_cases_monotonic(self):
        with self.assertRaises(Exception) as cm:
            parse(('Dagsetning,Smit_Samtals\n'
                   '2020-03-29,156\n'
                   '2020-03-30,16'), 'Smit_Samtals', 'cases')
        exceptions = cm.exception.args[0]
        exception = exceptions[0]
        self.assertRegex(str(exceptions[0]), 'cases is not monotonic')


if __name__ == '__main__':
    unittest.main()
