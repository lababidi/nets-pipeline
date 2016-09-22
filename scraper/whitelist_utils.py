import pandas as pd
'''
Basic line to convert the CSV to a more useful JSON
'''
pd.read_csv('scraper/whitelist_urls.csv',
            header=None,
            names=['name', 'url', 'type', 'lang'],
            index_col='name').to_json('whitelist.json', orient='index')
