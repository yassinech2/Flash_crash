import dask
from datetime import datetime
import os
import re 
import pandas as pd



dask.config.set(scheduler="processes")

@dask.delayed
def load_TRTH_trade(filename,
             tz_exchange="America/New_York",
             only_non_special_trades=True,
             only_regular_trading_hours=True,
             open_time="09:30:00",
             close_time="16:00:00",
             merge_sub_trades=True,
             drop_columns = True ):
    try:
        if re.search('(csv|csv\\.gz)$',filename):
            
            DF = pd.read_csv(filename)
        if re.search(r'arrow$',filename):
            DF = pd.read_arrow(filename)
        if re.search('parquet$',filename):
            DF = pd.read_parquet(filename)

    except Exception as e:
        print("load_TRTH_trade could not load "+filename)
     #   print(e)
        return None
    
    try:
        DF.shape
    except Exception as e: # DF does not exist
        print("DF does not exist")
        print(e)
        return None

    
    if DF.shape[0]==0:
        return None
    
    if only_non_special_trades:
        DF = DF[DF["trade-stringflag"]=="uncategorized"]
    if drop_columns==True: 
        DF.drop(columns=["trade-rawflag","trade-stringflag"],axis=1,inplace=True)
    
    DF.index = pd.to_datetime(DF["xltime"],unit="d",origin="1899-12-30",utc=True)
    DF.index = DF.index.tz_convert(tz_exchange)  # .P stands for Arca, which is based at New York
    DF.drop(columns="xltime",inplace=True)
    
    if only_regular_trading_hours:
        DF=DF.between_time(open_time,close_time)    # warning: ever heard e.g. about Thanksgivings?
    
    if merge_sub_trades:
           DF=DF.groupby(DF.index).agg(trade_price=pd.NamedAgg(column='trade-price', aggfunc='mean'),
                                       trade_volume=pd.NamedAgg(column='trade-volume', aggfunc='sum'))
    
    return DF



@dask.delayed
def load_TRTH_bbo(filename,
                  tz_exchange="America/New_York",
                  only_regular_trading_hours=True,
                  merge_sub_trades=True):
    try:
        if re.search(r'(csv|csv\.gz)$',filename):
            DF = pd.read_csv(filename)
        if re.search(r'arrow$',filename):
            DF = pd.read_arrow(filename)
        if re.search(r'parquet$',filename):
            DF = pd.read_parquet(filename) 
    except Exception as e:
        print("load_TRTH_bbo could not load "+filename)
        return None 
    try:
        DF.shape
    except Exception as e: # DF does not exist
        print("DF does not exist")
        print(e)
        return None

    if DF.shape[0]==0:
        return None
    
        
    DF.index = pd.to_datetime(DF["xltime"],unit="d",origin="1899-12-30",utc=True)
    DF.index = DF.index.tz_convert(tz_exchange)  # .P stands for Arca, which is based at New York
    DF.drop(columns="xltime",inplace=True)
    
    if only_regular_trading_hours:
        DF=DF.between_time("09:30:00","16:00:00")    # ever heard about Thanksgivings?
        
    if merge_sub_trades:
        DF=DF.groupby(DF.index).last()
    

        
    return DF

def extract_files(base_dir, ticker, start_date, end_date):
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    bbo_files = []
    trade_files = []

    paths_to_search = {
        'bbo': os.path.join(base_dir, ticker, 'bbo'),
        'trade': os.path.join(base_dir, ticker, 'trade')
    }

    for category, path in paths_to_search.items():
        for root, dirs, files in os.walk(path):
            current_dir = os.path.basename(root)
            try:
                dir_year, dir_month = current_dir.split('_')
                dir_date = datetime(int(dir_year), int(dir_month), 1)
                if start_date <= dir_date <= end_date:
                    for file in files:
                        file_date_str = '-'.join(file.split('-')[:3])
                        try:
                            file_date = datetime.strptime(file_date_str, "%Y-%m-%d")
                            if start_date <= file_date <= end_date:
                                full_path = os.path.join(root, file)
                                if category == 'bbo':
                                    bbo_files.append(full_path)
                                else:
                                    trade_files.append(full_path)
                        except ValueError:
                            pass
            except ValueError:
                pass

    return bbo_files, trade_files

