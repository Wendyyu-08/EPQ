import pandas as pd
import json as j
from datetime import datetime

from constants import PATH_CLEAN


# Generates value-added by Sector X in Country Y, to the rest of the world
def make_pivotal2():
    rf = pd.DataFrame(columns=['year','country','sector','value'])

    for year in range(1995,2021):
        print(f'Processing {year}')
        df = pd.read_parquet(f'{PATH_CLEAN}icio_{year}.parquet')
        df = df[df.input_country != df.output_country]
        df = df[~df.input_country.isin(['TLS','VA','Z','X','OUT'])]
        df = df[~df.output_sector.isin(['HFCE', 'NPISH','GGFC', 'GFCF', 'INVNT', 'DPABR', 'ALL'])] # excluding consumption angle

        df = df[['input_country','input_sector','value']]\
            .groupby(['input_country','input_sector'])\
                .sum()\
                    .sort_values(by='value',ascending=False)\
                        .reset_index()
        df = df[df.input_country != 'ROW'].reset_index(drop=True)
        df = df.rename(columns={'input_country':'country','input_sector':'sector'}).assign(year=year)[['year','country','sector','value']]
        
        if len(rf) > 0:
            rf = pd.concat([rf,df],axis=0,ignore_index=True)
        else:
            rf = df.copy()

    rf.to_parquet(f'output/pivotal2.parquet',index=False,compression='brotli')
    print(f'Wrote pivotal2.parquet: {len(rf):,.0f} rows')


# Generates value-consumed by Sector X in Country Y, from the rest of the world
def make_pivotal3():
    rf = pd.DataFrame(columns=['year','country','sector','value'])

    for year in range(1995,2021):
        print(f'Processing {year}')
        df = pd.read_parquet(f'{PATH_CLEAN}icio_{year}.parquet')
        df = df[df.input_country != df.output_country]
        df = df[~df.input_country.isin(['TLS','VA','Z','X','OUT'])]

        df = df[df.output_sector.isin(['HFCE','NPISH','GGFC','GFCF','INVNT','DPABR'])] # consumption angle

        df  = df[['output_country','output_sector','value']]\
            .groupby(['output_country','output_sector'])\
                .sum()\
                    .sort_values(by='value',ascending=False)\
                        .reset_index()
        df = df[df.output_country != 'ROW'].reset_index(drop=True)
        df = df.rename(columns={'output_country':'country','output_sector':'sector'}).assign(year=year)[['year','country','sector','value']]

        if len(rf) > 0:
            rf = pd.concat([rf,df],axis=0,ignore_index=True)
        else:
            rf = df.copy()

    rf.to_parquet(f'output/pivotal3.parquet',index=False,compression='brotli')
    print(f'Wrote pivotal3.parquet: {len(rf):,.0f} rows')


# Generates number of maritime and air connections by country
def make_pivotal4():
    cf = pd.read_csv('dep/country_iso.csv')
    MAP_ISO3 = dict(zip(cf['country'],cf['iso3']))

    df = pd.read_excel('/Users/theveshtheva/Downloads/data/raw/lpi_maritime.xlsx',usecols='A,D')
    df.columns = ['country','connections_maritime']
    df.connections_maritime = df.connections_maritime.astype(int)
    df.country = df.country.map(MAP_ISO3)

    tf = pd.read_excel('/Users/theveshtheva/Downloads/data/raw/lpi_aviation.xlsx',usecols='A,B')
    tf.columns = ['country','connections_air']
    tf.connections_air = tf.connections_air.astype(int)
    tf.country = tf.country.map(MAP_ISO3)

    df = pd.merge(tf,df,on='country',how='outer')
    # df['country_name'] = df['country'].map({v: k for k, v in MAP_ISO3.items()})
    for c in ['connections_air','connections_maritime']:
        df[c] = df[c].fillna(0).astype(int)
    df = pd.melt(df,id_vars=['country'],value_vars=['connections_air','connections_maritime'],var_name='mode',value_name='connections')
    df['mode'] = df['mode'].map({'connections_air':'air','connections_maritime':'maritime'})
    df = df[['country','mode','connections']]
    df.to_parquet('output/pivotal4.parquet',index=False,compression='brotli')


# Generates number of data centres and submarine cable landings by country
def make_pivotal5():
    cf = pd.read_csv('dep/country_iso.csv')
    MAP_ISO3 = dict(zip(cf['country'],cf['iso3']))

    MAP_COUNTRY_FIX = {
        'Ascension and Tristan da Cunha': 'Saint Helena, Ascension and Tristan da Cunha',
        'Brunei': 'Brunei Darussalam',
        'Cape Verde': 'Cabo Verde',
        'Dem. Rep.': 'Congo, Democratic Republic of the',
        'Iran': 'Iran, Islamic Republic of',
        'Micronesia': 'Micronesia, Federated States of',
        'Netherlands': 'Netherlands, Kingdom of the',
        'Rep.': 'Congo',
        'Russia': 'Russian Federation',
        'Saint Martin': 'Saint Martin (French part)',
        'Sint Eustatius and Saba': 'Bonaire, Sint Eustatius and Saba',
        'Sint Maarten': 'Sint Maarten (Dutch part)',
        'South Korea': 'Korea, Republic of',
        'Syria': 'Syrian Arab Republic',
        'Taiwan': 'Taiwan, Province of China',
        'Tanzania': 'Tanzania, United Republic of',
        'Turkey': 'Türkiye',
        'United Kingdom': 'United Kingdom of Great Britain and Northern Ireland',
        'United States': 'United States of America',
        'Venezuela': 'Venezuela, Bolivarian Republic of',
        'Vietnam': 'Viet Nam',
        'Virgin Islands (U.K.)': 'Virgin Islands (British)'
    }

    data = j.load(open('/Users/theveshtheva/Downloads/data/raw/cable_landings.json'))['features']
    df = pd.DataFrame(data)
    df['location'] = df['properties'].apply(lambda x: x['name'])
    df = df.drop(['type','properties','geometry'], axis=1)
    df[['location','country']] = df['location'].str.rsplit(',', n=1, expand=True)
    df.country = df.country.astype(str).str.strip()
    df.country = df.country.map(MAP_COUNTRY_FIX).fillna(df.country)
    df = df.assign(landings=1).groupby(['country']).sum(numeric_only=True).reset_index()
    df.country = df.country.map(MAP_ISO3)

    tf = pd.read_csv('/Users/theveshtheva/Downloads/data/raw/data_centres.csv',usecols=['iso3','data_centres'])\
    .rename(columns={'iso3':'country','data_centres':'data_centres'})

    df = pd.merge(df,tf,on='country',how='left')
    df.data_centres = df.data_centres.fillna(0).astype(int)
    df = pd.melt(df,id_vars=['country'],value_vars=['landings','data_centres'],
                var_name='variable',value_name='value')
    df.to_parquet('output/pivotal5.parquet',index=False,compression='brotli')


# Generates proportion of FX turnover by country
def make_pivotal6():
    cf = pd.read_csv('dep/currency_country.csv')
    MAP_CUR_ISO3 = dict(zip(cf['currency'],cf['country']))

    COLS = ['country']
    for y in [1992,1995,1998,2001,2004,2007,2010,2013,2016,2019,2022]:
        COLS += [f'value_{y}',f'proportion_{y}']

    df = pd.read_excel('/users/theveshtheva/Downloads/data/raw/bis_compiled.xlsx')
    df.currency = df.currency.map(MAP_CUR_ISO3)
    df.columns = COLS
    df = pd.melt(df,id_vars=['country'],value_vars=COLS[1:],var_name='measure',value_name='value')
    df[['measure','year']] = df['measure'].str.split('_',expand=True,n=1)
    df['year'] = df['year'].astype(int)
    df = df[(df['year'] > 1992) & (df['measure'] == 'proportion')]
    df = df[['country','year','value']].rename(columns={'value':'proportion'}).pivot(index='country',columns='year',values='proportion')

    # Apportion Euro
    EURO = ['DEU','FRA','ITA','NLD','BEL']
    SUM_1998 = df[df.index.isin(EURO)][1998].sum(axis=0)
    for c in EURO:
        for y in [2001,2004,2007,2010,2013,2016,2019,2022]:
            df.loc[c,y] = (df.loc[c,1998] / SUM_1998) * df.loc['EUR',y]
    df = df[~df.index.isin(['Total','EUR'])].reset_index()
    df = pd.melt(df,id_vars=['country'],value_vars=df.columns[1:],var_name='year',value_name='proportion')
    df.to_parquet('output/pivotal6.parquet',index=False,compression='brotli')


if __name__ == '__main__':
    START = datetime.now()
    print(f'\nStart: {START:%Y-%m-%d %H:%M:%S}\n')
    make_pivotal2()
    print('')
    make_pivotal3()
    print('')
    make_pivotal4()
    print('')
    make_pivotal5()
    print('')
    make_pivotal6()
    print(f'\nEnd: {datetime.now():%Y-%m-%d %H:%M:%S}')
    print(f'\nRuntime: {datetime.now() - START:%H:%M:%S}\n')