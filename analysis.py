import pandas as pd
import json as j
from datetime import datetime, date
from dateutil.relativedelta import relativedelta as rd

def check_missing_countries(df=None, MAP_ISO3=None):
    missing_countries = set(df['country']) - set(MAP_ISO3.keys())
    if missing_countries:
        print("Error: The following countries are missing from MAP_ISO3:")
        for country in sorted(missing_countries):
            print(f"- {country}")
        raise ValueError("Missing country mappings found")


# Generates main exporters of critical raw materials
def make_pivotal1():
    from dep.hs_list import CRM
    CRM = list(set(CRM))

    df = pd.DataFrame()

    for year in range(1995,2024):
        print(f'Processing {year}')
        tf = pd.read_parquet(f'data/baci/clean/baci_{year}.parquet')
        tf.hs = tf.hs.astype(str).str.zfill(6)
        tf = tf[tf.hs.isin(CRM)][['from','hs','value']].groupby(['from','hs']).sum().reset_index()
        tf = tf.pivot(index='from',columns='hs',values='value').fillna(0)
        for c in tf.columns: tf[c] = tf[c]/tf[c].max() * 100
        tf['pivotal1'] = tf.mean(axis=1).round(3)
        COLS = list(tf.columns)
        tf = tf.reset_index().assign(year=year)
        tf = tf[['year','from'] + COLS].rename(columns={'from':'country'})
        
        if len(df) > 0:
            df = pd.concat([df,tf],axis=0,ignore_index=True)
        else:
            df = tf.copy()

    df.to_parquet('output/pivotal1.parquet',index=False,compression='brotli')


# Generates value-added by Sector X in Country Y, to the rest of the world
def make_pivotal2():
    rf = pd.DataFrame(columns=['year','country','sector','value'])

    for year in range(1995,2021):
        print(f'Processing {year}')
        df = pd.read_parquet(f'data/icio/clean/icio_{year}.parquet')
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
        df = pd.read_parquet(f'data/icio/clean/icio_{year}.parquet')
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


# Generates number of maritime, air and land connections by country
def make_pivotal4():
    MAP_COUNTRY_FIX = {
        'British Virgin Islands': 'Virgin Islands (British)',
        'China, Hong Kong SAR': 'Hong Kong SAR',
        'China, Taiwan Province of': 'Taiwan ROC',
        'Cote d\'Ivoire': 'Côte d\'Ivoire',
        'Curacao': 'Curaçao',
        'Dem. People\'s Rep. of Korea': 'North Korea',
        'Dem. Rep. of the Congo': 'Congo, Democratic Republic of the',
        'Iran (Islamic Republic of)': 'Iran, Islamic Republic of',
        'Micronesia (Federated States of)': 'Micronesia, Federated States of',
        'Netherlands (Kingdom of the)': 'Netherlands',
        'Netherlands Antilles': 'Bonaire, Sint Eustatius and Saba',
        'Republic of Korea': 'South Korea',
        'Republic of Moldova': 'Moldova, Republic of',
        'Reunion': 'Réunion',
        'Russian Federation': 'Russia',
        'Saint Helena': 'Saint Helena, Ascension and Tristan da Cunha',
        'Serbia and Montenegro': 'Serbia',
        'Sudan (...2011)': 'Sudan',
        'Turkiye': 'Türkiye',
        'United Republic of Tanzania': 'Tanzania, United Republic of',
        'United States Virgin Islands': 'Virgin Islands (U.S.)',
        'Venezuela (Bolivarian Rep. of)': 'Venezuela, Bolivarian Republic of',
        'Wallis and Futuna Islands': 'Wallis and Futuna',
    }

    cf = pd.read_csv('dep/country_iso.csv')
    MAP_NAME_ISO3 = dict(zip(cf['country'],cf['iso3']))
    MAP_ISO2_ISO3 = dict(zip(cf['iso2'],cf['iso3']))

    df = pd.read_csv('data/lsci.csv')
    df.columns = ['country'] + [date(2006,1,1) + rd(months=i*3) for i in range(len(df.columns)-1)]
    df.country = df.country.map(MAP_COUNTRY_FIX).fillna(df.country)
    check_missing_countries(df, MAP_NAME_ISO3)
    df.country = df.country.map(MAP_NAME_ISO3)
    df = pd.melt(df,id_vars=['country'],value_vars=list(df.columns[1:]),var_name='year',value_name='maritime')
    df.year = pd.to_datetime(df.year).dt.year
    df = df.groupby(['country','year']).mean(numeric_only=True).reset_index()
    df = df[df.year < 2024].pivot(index='country',columns='year',values='maritime').reset_index()
    for c in df.columns[1:]:
        df[c] = df[c].fillna(0) / df[c].max() * 100
    df = pd.melt(df,id_vars='country',value_vars=list(df.columns[1:]),var_name='year',value_name='maritime')

    tf = pd.read_excel('data/lpi_aviation.xlsx',usecols='A,B')
    tf.columns = ['country','air']
    check_missing_countries(tf, MAP_NAME_ISO3)
    tf.country = tf.country.map(MAP_NAME_ISO3)
    df = pd.merge(df,tf,on='country',how='left')
    df.air = df.air.fillna(0).astype(int)

    tf = pd.read_csv('data/land_borders.csv',usecols=['flagCode','DistinctLandNeighbors'])
    tf.columns = ['country','land']
    check_missing_countries(tf, MAP_ISO2_ISO3)
    tf.country = tf.country.map(MAP_ISO2_ISO3)
    df = pd.merge(df,tf,on='country',how='left')
    df.land = df.land.fillna(0).astype(int)
    for c in ['air','land']:
        df[c] = df[c] / df[c].max() * 100
    df['pivotal4'] = 0.8* df.maritime + 0.18* df.air + 0.02* df.land
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
        'Netherlands': 'Netherlands',
        'Rep.': 'Congo',
        'Saint Martin': 'Saint Martin (French part)',
        'Sint Eustatius and Saba': 'Bonaire, Sint Eustatius and Saba',
        'Sint Maarten': 'Sint Maarten (Dutch part)',
        'Syria': 'Syrian Arab Republic',
        'Taiwan': 'Taiwan ROC',
        'Tanzania': 'Tanzania, United Republic of',
        'Turkey': 'Türkiye',
        'United Kingdom': 'United Kingdom',
        'United States': 'United States',
        'Venezuela': 'Venezuela, Bolivarian Republic of',
        'Vietnam': 'Viet Nam',
        'Virgin Islands (U.K.)': 'Virgin Islands (British)'
    }

    data = j.load(open('data/cable_landings.json'))['features']
    df = pd.DataFrame(data)
    df['location'] = df['properties'].apply(lambda x: x['name'])
    df = df.drop(['type','properties','geometry'], axis=1)
    df[['location','country']] = df['location'].str.rsplit(',', n=1, expand=True)
    df.country = df.country.astype(str).str.strip()
    df.country = df.country.map(MAP_COUNTRY_FIX).fillna(df.country)
    df = df.assign(landings=1).groupby(['country']).sum(numeric_only=True).reset_index()
    check_missing_countries(df, MAP_ISO3)
    df.country = df.country.map(MAP_ISO3)

    tf = pd.read_csv('data/data_centres.csv',usecols=['iso3','data_centres'])\
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

    df = pd.read_excel('data/bis_compiled.xlsx')
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
    print('')
    make_pivotal1()
    print('')
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
    print(f'\nRuntime: {datetime.now() - START}\n')