import yfinance as yf
import os, contextlib
from loguru import logger
import shutil
from os.path import join
import pandas as pd


# download data
if False:
    offset = 0
    limit = 3000
    period = 'max' # valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max

    data = pd.read_csv("http://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt", sep='|')
    data_clean = data[data['Test Issue'] == 'N']
    symbols = data_clean['NASDAQ Symbol'].tolist()
    logger.info('total number of symbols traded = {}'.format(len(symbols)))

    limit = limit if limit else len(symbols)
    end = min(offset + limit, len(symbols))
    is_valid = [False] * len(symbols)
    # force silencing of verbose API
    with open(os.devnull, 'w') as devnull:
        with contextlib.redirect_stdout(devnull):
            for i in range(offset, end):
                s = symbols[i]
                download_filepath = 'hist/{}.csv'.format(s)

                if os.path.exists(download_filepath):
                    logger.info("{} downloaded already; skip".format(s))
                    continue

                try:
                    data = yf.download(s, period=period)
                except Exception as e:
                    logger.error(f"error when downloading {e}; skip")
                    continue
                    
                if len(data.index) == 0:
                    logger.info("{} data.index length is 0; skip".format(s))
                    continue

                is_valid[i] = True
                data.to_csv(download_filepath)

                logger.info(f"downloaded {download_filepath}")


    print('Total number of valid symbols downloaded = {}'.format(sum(is_valid)))

    valid_data = data_clean[is_valid]
    valid_data.to_csv('symbols_valid_meta.csv', index=False)

if True:
    valid_data = pd.read_csv('./symbols_valid_meta.csv')

etfs = valid_data[valid_data['ETF'] == 'Y']['NASDAQ Symbol'].tolist()
stocks = valid_data[valid_data['ETF'] == 'N']['NASDAQ Symbol'].tolist()

def move_symbols(symbols, dest):
    for s in symbols:
        filepath = '{}.csv'.format(s)
        src = join('./hist', filepath)
        if not os.path.exists(src):
            logger.info(f"{src} does not exist; skip")
            continue
        dst = join(dest, filepath)
        shutil.move(src, dst)
        
move_symbols(etfs, "./etfs")
move_symbols(stocks, "./stocks")



