import pandas as pd
import numpy as np
from datetime import datetime
from matplotlib import pyplot as plt
import seaborn as sb

from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

iso = pd.read_csv('dep/country_iso.csv',usecols=['country','iso3'])
MAP_ISO3 = dict(zip(iso.iso3,iso.country))
MAP_ISO3['ROW'] = 'Rest of World'
MAP_ISO_REV = {v:k for k,v in MAP_ISO3.items()}


def pivotal1_heatmap():
    df = pd.read_parquet('output/pivotal1.parquet',columns=['year','country','pivotal1']).rename(columns={'pivotal1':'value'})
    df = df.pivot(index='country',columns='year',values='value').reset_index()
    for c in df.columns[1:]: df[c] = df[c]/df[c].max() * 100
    df.to_parquet('output/pivotal_index1.parquet',index=False,compression='brotli')
    df = df[['country'] + [1999 + i*3 for i in range(9)]].sort_values(by=2023,ascending=False)
    df.country = df.country.str.replace('SUI','CHE').map(MAP_ISO3) + '  '
    REPL = [('United Arab Emirates','UAE'),('Congo, Democratic Republic of the','DR Congo')]
    for r in REPL: df.country = df.country.str.replace(r[0],r[1])
    df = df.set_index('country')

    fig, ax = plt.subplots(figsize=[8,8])  # width, height

    # heatmap
    sb.heatmap(df.head(20),
            annot=True, fmt=",.1f",
            vmin=0,vmax=40,
            annot_kws={'fontsize': 12},
            cmap='Blues',
            cbar=False,
            cbar_kws={"shrink": .9},
            linewidths=0.3,
            linecolor='#cecece',
            ax=ax)
    # add grid
    ax.set_ylabel('')
    ax.set_xlabel('')
    ax.set_facecolor('white')

    # ticks
    plt.yticks(rotation=0)
    ax.tick_params(axis=u'both', which=u'both',
                length=0, labelsize=12, 
                labelbottom=False, labeltop=True,
                bottom=False, top=False)

    plt.savefig('typeset/dataviz/results_pivotal1_snapshot.eps', format='eps', bbox_inches='tight')
    plt.close()
    

def pivotal2_heatmap():
    df = pd.read_parquet('output/pivotal2.parquet')
    df = df.groupby(['year','country']).sum(numeric_only=True).reset_index()
    df = df.pivot(index='country',columns='year',values='value').reset_index()
    for c in df.columns[1:]: df[c] = df[c]/df[c].max() * 100
    df.to_parquet('output/pivotal_index2.parquet',index=False,compression='brotli')
    df = df[['country'] + [1999 + i*3 for i in range(8)]].sort_values(by=2020,ascending=False)
    df.country = df.country.str.replace('SUI','CHE').map(MAP_ISO3) + '  '
    df.country = df.country.str.replace('United Arab Emirates','UAE').str.replace('Taiwan ROC','Taiwan')
    df = df.set_index('country')

    fig, ax = plt.subplots(figsize=[8,8])  # width, height

    # heatmap
    sb.heatmap(df.head(20),
            annot=True, fmt=",.1f",
            vmin=0,vmax=40,
            annot_kws={'fontsize': 12},
            cmap='Blues',
            cbar=False,
            cbar_kws={"shrink": .9},
            linewidths=0.3,
            linecolor='#cecece',
            ax=ax)
    # add grid
    ax.set_ylabel('')
    ax.set_xlabel('')
    ax.set_facecolor('white')

    # ticks
    plt.yticks(rotation=0)
    ax.tick_params(axis=u'both', which=u'both',
                length=0, labelsize=12, 
                labelbottom=False, labeltop=True,
                bottom=False, top=False)

    plt.savefig('typeset/dataviz/results_pivotal2_snapshot.eps', format='eps', bbox_inches='tight')
    plt.close()


def pivotal3_heatmap():
    df = pd.read_parquet('output/pivotal3.parquet')
    df = df.groupby(['year','country']).sum(numeric_only=True).reset_index()
    df = df.pivot(index='country',columns='year',values='value').reset_index()
    for c in df.columns[1:]: df[c] = df[c]/df[c].max() * 100
    df.to_parquet('output/pivotal_index3.parquet',index=False,compression='brotli')
    df = df[['country'] + [1999 + i*3 for i in range(8)]].sort_values(by=2020,ascending=False)
    df.country = df.country.str.replace('SUI','CHE').map(MAP_ISO3) + '  '
    df.country = df.country.str.replace('United Arab Emirates','UAE')
    df = df.set_index('country')

    fig, ax = plt.subplots(figsize=[8,8])  # width, height

    # heatmap
    sb.heatmap(df.head(20),
            annot=True, fmt=",.1f",
            vmin=0,vmax=40,
            annot_kws={'fontsize': 12},
            cmap='Blues',
            cbar=False,
            cbar_kws={"shrink": .9},
            linewidths=0.3,
            linecolor='#cecece',
            ax=ax)
    # add grid
    ax.set_ylabel('')
    ax.set_xlabel('')
    ax.set_facecolor('white')

    # ticks
    plt.yticks(rotation=0)
    ax.tick_params(axis=u'both', which=u'both',
                length=0, labelsize=12, 
                labelbottom=False, labeltop=True,
                bottom=False, top=False)

    plt.savefig('typeset/dataviz/results_pivotal3_snapshot.eps', format='eps', bbox_inches='tight')
    plt.close()


def pivotal4_heatmap():
    df = pd.read_parquet('output/pivotal4.parquet',columns=['country','year','pivotal4'])
    df = df.pivot(index='country',columns='year',values='pivotal4').reset_index()
    for c in df.columns[1:]: df[c] = df[c]/df[c].max() * 100
    df.to_parquet('output/pivotal_index4.parquet',index=False,compression='brotli')
    df = df[['country',2006] + [2008 + i*3 for i in range(6)]].sort_values(by=2023,ascending=False)
    df.country = df.country.str.replace('SUI','CHE').map(MAP_ISO3) + '  '
    df.country = df.country.str.replace('United Arab Emirates','UAE').str.replace('Taiwan ROC','Taiwan')
    df = df.set_index('country')

    fig, ax = plt.subplots(figsize=[10, 10])  # width, height

    # heatmap
    sb.heatmap(df[df[2023] >= 30],
            annot=True, fmt=",.1f",
            vmin=30,vmax=65,
            annot_kws={'fontsize': 12},
            cmap='Blues',
            cbar=False,
            cbar_kws={"shrink": .9},
            linewidths=0.3,
            linecolor='#cecece',
            ax=ax)
    # add grid
    ax.set_ylabel('')
    ax.set_xlabel('')
    ax.set_facecolor('white')

    # ticks
    plt.yticks(rotation=0)
    ax.tick_params(axis=u'both', which=u'both',
                length=0, labelsize=12, 
                labelbottom=False, labeltop=True,
                bottom=False, top=False)

    plt.savefig('typeset/dataviz/results_pivotal4_snapshot.eps', format='eps', bbox_inches='tight')
    plt.close()


def pivotal5_barchart():
    df = pd.read_parquet('output/pivotal5.parquet')
    df = df.pivot(index='country',columns='variable',values='value').reset_index()
    for c in ['data_centres','landings']: df[f'{c}_index'] = df[c]/df[c].max() * 100
    df['pivotal5'] = (df['data_centres_index'] + df['landings_index'])/2
    df = df.sort_values(by='pivotal5',ascending=True).reset_index(drop=True)
    df.to_parquet('output/pivotal_index5.parquet',index=False,compression='brotli')
    df.country = df.country.str.replace('SUI','CHE').map(MAP_ISO3)

    plt.rcParams.update({'font.size': 10,
                        'font.family': 'sans-serif',
                        'grid.linestyle': 'dashed'})
    plt.rcParams["figure.figsize"] = [5,6]
    plt.rcParams["figure.autolayout"] = True
    fig, ax = plt.subplots()

    v = 'pivotal5'
    N = 20
    dfp = df.tail(N).copy().set_index('country')
    col_pos = sb.color_palette('Blues', n_colors=N).as_hex()
    dfp.plot(kind='barh', width=0.7, y=v, edgecolor='black', lw=0, color=col_pos, ax=ax)

    # plot-wide adjustments
    ax.set_title(f'',linespacing=1,fontsize=11)
    for b in ['top','right','bottom']: ax.spines[b].set_visible(False)
    ax.spines['left'].set_color('#cccccc')
    ax.get_legend().remove()
    ax.set_axisbelow(True)
    ax.tick_params(axis=u'both', which=u'both',length=0)

    # y-axis adjustments
    ax.set_ylabel('')

    # x-axis adjustments
    ax.set_xlabel('')
    ax.xaxis.grid(False)
    ax.get_xaxis().set_visible(False)
    for c in ax.containers:
            labels = [f"  {dfp[v].iloc[i]:,.1f}" for i in range(len(dfp))]
    ax.bar_label(c, labels=labels,fontsize=10)

    plt.savefig('typeset/dataviz/results_pivotal5_snapshot.eps', format='eps', bbox_inches='tight')
    plt.close()


def pivotal6_heatmap():
    df = pd.read_parquet('output/pivotal6.parquet')
    df.country = df.country.str.replace('SUI','CHE').map(MAP_ISO3)
    df.country = df.country.str.replace('United Arab Emirates','UAE').str.replace('Taiwan ROC','Taiwan')
    df = df.pivot(index='country',columns='year',values='proportion')

    for c in df.columns: df[c] = df[c]/df[c].max() * 100
    df = df.reset_index().sort_values(by=2022,ascending=False).reset_index(drop=True).set_index('country')
    df.reset_index().to_parquet('output/pivotal_index6.parquet',index=False,compression='brotli')
    df.index = df.index + '  '

    fig, ax = plt.subplots(figsize=[10, 10])  # width, height

    # heatmap
    sb.heatmap(df[df[2022] >= 1],
            annot=True, fmt=",.1f",
            vmin=0,vmax=35,
            annot_kws={'fontsize': 12},
            cmap='Blues',
            cbar=False,
            cbar_kws={"shrink": .9},
            linewidths=0.3,
            linecolor='#cecece',
            ax=ax)
    # add grid
    ax.set_ylabel('')
    ax.set_xlabel('')
    ax.set_facecolor('white')

    # ticks
    plt.yticks(rotation=0)
    ax.tick_params(axis=u'both', which=u'both',
                length=0, labelsize=12, 
                labelbottom=False, labeltop=True,
                bottom=False, top=False)

    plt.savefig('typeset/dataviz/results_pivotal6_snapshot.eps', format='eps', bbox_inches='tight')
    plt.close()


def pivotal_pca_barchart():
    df = pd.read_parquet('output/pivotal_index1.parquet')
    df = df[['country'] + [df.columns[-1]]].replace('SUI','CHE').fillna(0)
    df = df.rename(columns={df.columns[-1]:'pivotal1'})

    for i in range(2,7):
        tf = pd.read_parquet(f'output/pivotal_index{i}.parquet')
        if i == 6:
            tf.country = tf.country.map(MAP_ISO_REV)
        tf = tf[['country'] + [tf.columns[-1]]].replace('SUI','CHE').fillna(0)
        tf = tf.rename(columns={tf.columns[-1]:f'pivotal{i}'})
        df = pd.merge(df,tf,on='country',how='left').fillna(0)


    pivotal_cols = [col for col in df.columns if 'pivotal' in col]
    X = df[pivotal_cols]
    scaled_data = StandardScaler().fit_transform(X) # Standardize the data

    # Perform PCA with 1 component, then add it back to the original df
    pca = PCA(n_components=1)
    pivotal_values = pca.fit_transform(scaled_data)
    df['pivotal_pca'] = pivotal_values
    df['pivotal_pca'] = 100 * (df['pivotal_pca'] - df['pivotal_pca'].min()) / (df['pivotal_pca'].max() - df['pivotal_pca'].min())
    df = df.sort_values(by='pivotal_pca',ascending=True)
    df.to_parquet('output/pivotal_index.parquet',index=False,compression='brotli')
    df.pivotal_pca = df.pivotal_pca.round(2)
    
    print(f"Variance explained by 1st principal component: {pca.explained_variance_ratio_[0]*100:.2f}%")
    df['country'] = df.country.map(MAP_ISO3)

    plt.rcParams.update({'font.size': 10,
                        'font.family': 'sans-serif',
                        'grid.linestyle': 'dashed'})
    plt.rcParams["figure.figsize"] = [5,6]
    plt.rcParams["figure.autolayout"] = True
    fig, ax = plt.subplots()

    v = 'pivotal_pca'
    N = 20
    dfp = df.tail(N).copy().set_index('country')
    col_pos = sb.color_palette('Blues', n_colors=N).as_hex()
    dfp.plot(kind='barh', width=0.7, y=v, edgecolor='black', lw=0, color=col_pos, ax=ax)

    # plot-wide adjustments
    ax.set_title(f'',linespacing=1,fontsize=11)
    for b in ['top','right','bottom']: ax.spines[b].set_visible(False)
    ax.spines['left'].set_color('#cccccc')
    ax.get_legend().remove()
    ax.set_axisbelow(True)
    ax.tick_params(axis=u'both', which=u'both',length=0)

    # y-axis adjustments
    ax.set_ylabel('')

    # x-axis adjustments
    ax.set_xlabel('')
    ax.xaxis.grid(False)
    ax.get_xaxis().set_visible(False)
    for c in ax.containers:
        labels = [f"  {dfp[v].iloc[i]:,.1f}" for i in range(len(dfp))]
        ax.bar_label(c, labels=labels,fontsize=10)

    plt.savefig('typeset/dataviz/results_pivotal_pca.eps', format='eps', bbox_inches='tight')
    plt.close()


if __name__ == '__main__':
    print('')
    pivotal1_heatmap()
    pivotal2_heatmap()
    pivotal3_heatmap()
    pivotal4_heatmap()
    pivotal5_barchart()
    pivotal6_heatmap()
    pivotal_pca_barchart()
    print('')