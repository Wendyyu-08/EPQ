import pandas as pd
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


if __name__ == '__main__':
    print(f'\nStart: {datetime.now():%Y-%m-%d %H:%M:%S}\n')
    make_pivotal2()
    print('')
    make_pivotal3()
    print(f'\nEnd: {datetime.now():%Y-%m-%d %H:%M:%S}\n')