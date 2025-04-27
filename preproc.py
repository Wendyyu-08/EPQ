import pandas as pd
from datetime import datetime


def baci(YEARS = []):
    
    COL_RENAME = {
        't':'year',
        'i':'from',
        'j':'to',
        'k':'hs',
        'v':'value',
        'q':'volume'
    }

    mf = pd.read_csv('data/baci/lookup_country.csv')
    MAP_ISO3 = dict(zip(mf['country_code'],mf['country_iso3']))

    for year in YEARS:
        df = pd.read_csv(f'data/baci/raw/BACI_HS92_Y{year}_V202501.csv')
        df = df.rename(columns=COL_RENAME)
        for c in ['from','to']: df[c] = df[c].map(MAP_ISO3)
        assert len(df[df['from'].isna() | df['to'].isna()]) == 0, f'{year} has {len(df[df["from"].isna() | df["to"].isna()])} rows with missing country codes'
        df.to_parquet(f'data/baci/clean/baci_{year}.parquet',index=False,compression='zstd')


def icio(YEARS = []):

    for year in YEARS:
        df = pd.read_csv(f'data/icio/raw/{year}_SML.csv').rename(columns={'V1': 'input'})
        df = pd.melt(df, id_vars=['input'], value_vars=df.columns[1:], var_name='output', value_name='value')

        df.loc[~df.input.str.contains('_'), 'input'] = df.input + '_ALL'
        df.loc[~df.output.str.contains('_'), 'output'] = df.output + '_ALL'
        df[['input_country','input_sector']] = df['input'].str.split('_', expand=True, n=1)
        df[['output_country','output_sector']] = df['output'].str.split('_', expand=True, n=1)
        
        df = df[['input_country','input_sector','output_country','output_sector','value']]
        df.to_parquet(f'data/icio/clean/icio_{year}.parquet',index=False,compression='brotli')
        print(f'Wrote icio_{year}.parquet: {len(df):,.0f} rows')


if __name__ == '__main__':
    print(f'\nStart: {datetime.now():%Y-%m-%d %H:%M:%S}\n')
    icio(YEARS = list(range(1995,2021)))
    baci(YEARS = list(range(1995,2024)))
    print(f'\nEnd: {datetime.now():%Y-%m-%d %H:%M:%S}\n')