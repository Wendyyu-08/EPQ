import pandas as pd
from datetime import datetime

from constants import PATH_RAW, PATH_CLEAN


def icio(YEARS = []):

    for year in YEARS:
        df = pd.read_csv(f'{PATH_RAW}{year}_SML.csv').rename(columns={'V1': 'input'})
        df = pd.melt(df, id_vars=['input'], value_vars=df.columns[1:], var_name='output', value_name='value')

        df.loc[~df.input.str.contains('_'), 'input'] = df.input + '_ALL'
        df.loc[~df.output.str.contains('_'), 'output'] = df.output + '_ALL'
        df[['input_country','input_sector']] = df['input'].str.split('_', expand=True, n=1)
        df[['output_country','output_sector']] = df['output'].str.split('_', expand=True, n=1)
        
        df = df[['input_country','input_sector','output_country','output_sector','value']]
        df.to_parquet(f'{PATH_CLEAN}icio_{year}.parquet',index=False,compression='brotli')
        print(f'Wrote icio_{year}.parquet: {len(df):,.0f} rows')


if __name__ == '__main__':
    print(f'\nStart: {datetime.now():%Y-%m-%d %H:%M:%S}\n')
    icio(YEARS = list(range(1995,2021)))
    print(f'\nEnd: {datetime.now():%Y-%m-%d %H:%M:%S}\n')