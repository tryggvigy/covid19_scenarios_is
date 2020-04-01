import sys
import requests
import io
from pathlib import Path
import pandas as pd

URL_CASES = "https://raw.githubusercontent.com/tryggvigy/CoronaWatchIS/master/data/covid_in_is.cvs"
URL_HOSPITALIZED = "https://raw.githubusercontent.com/tryggvigy/CoronaWatchIS/master/data/covid_in_is_hosp.cvs"

def parse_csv(url, input_data_col, data_col, filename, cumsum=False):
    r = requests.get(url)
    if not r.ok:
        print(f"Failed to fetch {url}", file=sys.stderr)
        exit(1)
        r.close()

    fd = io.StringIO(r.text)
    df = pd.read_csv(fd, usecols = ['Dagsetning' , input_data_col], header=0)

    # set column header names
    df.columns = ['date', data_col]

    # set date format YYYY-mm-dd
    df['date'] = pd.to_datetime(df.date)

    # Apply cumulative sum to data
    if cumsum: df[data_col] = df[data_col].cumsum()

    print(df)

    df.to_csv(str(Path('data', filename)), index=False)


def build_data():
    # create data folder
    Path('data').mkdir(parents=True, exist_ok=True)

    print('Downloading')
    parse_csv(URL_CASES, 'Smit_Samtals', 'cases', 'cumulative_cases.cvs')
    parse_csv(URL_CASES, 'Dauðsföll', 'deaths', 'cumulative_deaths.cvs', cumsum=True)
    parse_csv(URL_CASES, 'Batnað_Samtals', 'recovered', 'cumulative_recovered.cvs')
    parse_csv(URL_HOSPITALIZED, 'Spitali_Samtals', 'hospitalized', 'cumulative_hospitalized.cvs')
    # Looks like this column should not be cumulative sum because Gjosgaesal_Samtals
    # also exists but doesn't make sense (has no data until 2020-03-27, then spikes to 11).
    # This is the cumulative sum column with the correct data.
    parse_csv(URL_HOSPITALIZED, 'Gjorgaesla', 'ICU', 'cumulative_icu.cvs')

    print('Success')

if __name__ == '__main__':
    build_data()
