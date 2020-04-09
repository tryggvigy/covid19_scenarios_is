import sys
import requests
import io
from pathlib import Path
import pandas as pd
from pandas_schema import Column, Schema
from pandas_schema.validation import \
    DateFormatValidation, \
    LeadingWhitespaceValidation, \
    TrailingWhitespaceValidation, \
    CustomSeriesValidation, \
    CanCallValidation, \
    CanConvertValidation

URL_CASES = "https://raw.githubusercontent.com/tryggvigy/CoronaWatchIS/master/data/covid_in_is.cvs"
URL_HOSPITALIZED = "https://raw.githubusercontent.com/tryggvigy/CoronaWatchIS/master/data/covid_in_is_hosp.cvs"

date_validator = Column('date', [
    LeadingWhitespaceValidation(),
    TrailingWhitespaceValidation(),
    DateFormatValidation("%Y-%m-%d") & CustomSeriesValidation(
        lambda x: x.is_monotonic_increasing and x.is_unique,
        'date is not monotonic')
])

default_value_validators = [
    LeadingWhitespaceValidation(),
    TrailingWhitespaceValidation()
]

schemas_by_key = {
    'cases':
    Schema([
        date_validator,
        Column('cases', [
            *default_value_validators,
            CanConvertValidation(int) & CustomSeriesValidation(
                lambda x: x.is_monotonic_increasing, 'cases is not monotonic')
        ])
    ]),
    'deaths':
    Schema([
        date_validator,
        Column('deaths', [
            *default_value_validators,
            CanConvertValidation(int) & CustomSeriesValidation(
                lambda x: x.is_monotonic_increasing, 'deaths is not monotonic')
        ])
    ]),
    'recovered':
    Schema([date_validator,
            Column('recovered', [*default_value_validators])]),
    'hospitalized':
    Schema([
        date_validator,
        Column('hospitalized',
               [*default_value_validators,
                CanConvertValidation(int)])
    ]),
    'ICU':
    Schema([
        date_validator,
        Column('ICU', [*default_value_validators,
                       CanConvertValidation(int)])
    ])
}


def validate(df):
    key = df.columns[1]
    violations = schemas_by_key[key].validate(df)

    for violation in violations:
        print(violation, file=sys.stderr)

    if len(violations):
        print(f"Validation on {key} failed", file=sys.stderr)
        raise Exception(violations)


def parse(csv_text, input_data_col, data_col, cumsum=False):
    fd = io.StringIO(csv_text)
    df = pd.read_csv(fd, usecols=['Dagsetning', input_data_col], header=0)

    # set column header names
    df.columns = ['date', data_col]

    # set date format YYYY-mm-dd
    df['date'] = pd.to_datetime(df.date).dt.date

    # make nullable integer
    df[data_col] = df[data_col].astype('Int64')

    # Apply cumulative sum to data
    if cumsum: df[data_col] = df[data_col].cumsum()

    validate(df)

    return df


def parse_csv(url, input_data_col, data_col, filename, cumsum=False):
    r = requests.get(url)
    if not r.ok:
        print(f"Failed to fetch {url}", file=sys.stderr)
        exit(1)
        r.close()

    df = parse(r.text, input_data_col, data_col, cumsum)

    df.to_csv(str(Path('data', filename)), index=False)


def build_data():
    # create data folder
    Path('data').mkdir(parents=True, exist_ok=True)

    print('Downloading')
    parse_csv(URL_CASES, 'Smit_Samtals', 'cases', 'cumulative_cases.cvs')
    parse_csv(URL_CASES, 'Dauðsföll_Samtals', 'deaths',
              'cumulative_deaths.cvs')
    parse_csv(URL_CASES, 'Batnað_Samtals', 'recovered',
              'cumulative_recovered.cvs')
    # should not be cumulative counts
    parse_csv(URL_HOSPITALIZED, 'Inniliggjandi', 'hospitalized',
              'cumulative_hospitalized.cvs')
    # Looks like this column should not be cumulative sum because Gjosgaesal_Samtals
    # also exists but doesn't make sense (has no data until 2020-03-27, then spikes to 11).
    # This is the cumulative sum column with the correct data.
    parse_csv(URL_HOSPITALIZED, 'Gjorgaesla', 'ICU', 'cumulative_icu.cvs')
    print('Success')


if __name__ == '__main__':
    build_data()
