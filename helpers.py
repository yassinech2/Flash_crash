import tarfile
import os
import shutil
from datetime import datetime
import re 
from dask import delayed,compute
import dask
import vaex
import pytz
import matplotlib.pyplot as plt
import plotly.graph_objs as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np

from multiprocessing import Pool
import scipy.signal
import statsmodels.api as sm
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
    """Loads a TRTH trade file and returns a dataframe.
    The dataframe has a datetime index, which is converted to the exchange timezone.
    The dataframe has the following columns:
    - trade-price
    - trade-volume
    - trade-exchange
    - trade-rawflag
    - trade-stringflag
    - xltime (the original xltime)
    
    Parameters
    ----------
    filename : (str) The filename of the trade file.
    tz_exchange : (str) The timezone of the exchange.
    only_non_special_trades : (bool)    If True, only the non-special trades are returned.
    only_regular_trading_hours : (bool)    If True, only the regular trading hours are returned.
    open_time : (str) The opening time of the exchange.
    close_time : (str) The closing time of the exchange.
    merge_sub_trades : (bool)     If True, subtrades are merged.
    drop_columns : (bool)     If True, drop columns.
    """
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
    if drop_columns: 
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
    """Loads a TRTH bbo file and returns a dataframe.
    The dataframe has a datetime index, which is converted to the exchange timezone.
    The dataframe has the following columns:
    - bid-price
    - bid-volume
    - ask-price
    - ask-volume
    - bid-exchange
    - ask-exchange
    - bid-count
    - ask-count
    - bid-rawflag
    - ask-rawflag
    - bid-stringflag
    - ask-stringflag
    - xltime (the original xltime)
    
    Parameters
    ----------
    filename : (str) The filename of the bbo file.
    tz_exchange : (str) The timezone of the exchange.
    only_regular_trading_hours : (bool)    If True, only the regular trading hours are returned.
    merge_sub_trades : (bool)     If True, subtrades are merged.
    """
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
    """Extracts the files for a given ticker and date range.
    
    Parameters
    ----------
    base_dir : (str) The base directory where the data is stored.
    ticker : (str) The ticker.
    start_date : (str) The start date in the format YYYY-MM-DD.
    end_date : (str) The end date in the format YYYY-MM-DD.
    
    Returns
    -------
    bbo_files : (list) The list of bbo files.
    trade_files : (list) The list of trade files.
    """
    if start_date is None:
        start_date = '2009-12-01'
    if end_date is None:
        end_date = '2012-01-01'

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


def list_file_types_in_tar(tar_path):
    """ Function to list file types in a tar archive.\n
        Parameters
        ----------
        tar_path : (str) The path to the tar archive.\n
        Returns
        -------
        file_types : (list) The list of file types."""
    file_types = []
    with tarfile.open(tar_path, "r") as tar:
        for member in tar.getmembers():
            if member.isfile():
                _, ext = os.path.splitext(member.name)
                file_types.append(ext.lower())
    return file_types

def createFoldersGroupingYnMonthfiles(directories):
    """ Function to create folders grouping files by year and month.\n
        Parameters:
        ----------
        directories : (list) The list of directories.\n
        Returns
        -------
        None"""
    for dir in directories:
        files = os.listdir(dir)
        for file in files:
            file_path = os.path.join(dir, file)
            # Check if the path is a file
            if os.path.isfile(file_path):
                date_part = file.split("-")[:2]  # Taking the year and month
                date_folder = "_".join(date_part)
                date_dir = os.path.join(dir, date_folder)
                if not os.path.exists(date_dir):
                    os.makedirs(date_dir)
                
                # Construct the destination path
                dest_path = os.path.join(date_dir, file)   
        
                # Move the file
                shutil.move(file_path, dest_path)

def read_TRTH_files(trade_files):
    delayed_dfs = []
    for file in trade_files:
        delayed_df = delayed(load_TRTH_trade)(file,
                                              only_regular_trading_hours=True,
                                              merge_sub_trades=True,
                                              drop_columns=True)
        delayed_dfs.append(delayed_df)
    dfs = compute(*delayed_dfs)
    final_df = pd.concat(dfs, ignore_index=False)
    final_df = final_df.sort_index()
    return final_df

def read_bbo_files(trade_files):
    delayed_dfs = []
    for file in trade_files:
        delayed_df = delayed(load_TRTH_bbo)(file)
        delayed_dfs.append(delayed_df)
    dfs = compute(*delayed_dfs)
    final_df = pd.concat(dfs, ignore_index=False)
    final_df = final_df.sort_index()
    return final_df


@dask.delayed
def load_merge_trade_bbo(ticker,
                         date_start,
                         date_end,
                         dirBase,
                         suffix="parquet",
                         suffix_save=None,
                         dirSaveBase = None ,
                         saveOnly=False,
                         doSave=False):
    """
    This function allows to merge trade and bbo together of a given ticker, 
    We should precise the destination to save the merged data 
    The date merged can be done in any period we would like to test on
    By default the method returns the merged series, but can save or only save the merged series 
    Given a location of the saving directory, the suffix is by default a parquet file. 
    """
    bbo_files,trade_files  = extract_files(dirBase,ticker,date_start,date_end)
    trades = read_TRTH_files(trade_files)
    bbos = read_bbo_files(bbo_files)
    try:
        trades.shape + bbos.shape
    except:
        return None
    
    events=trades.join(bbos,how="outer")
    
    if doSave:
        dirSave=os.path.join(dirSaveBase,ticker)
        if not os.path.isdir(dirSave):
            os.makedirs(dirSave)

        if suffix_save:
            suffix=suffix_save
        
        file_events=dirSave+"/"+date_end +"-"+ date_end +  "-events"+"."+suffix
       # pdb.set_trace()

        saved=False
        if suffix == "arrow":
            events= vaex.from_pandas(events,copy_index=True)
            events.export_arrow(file_events)
            saved=True
        
        elif suffix == "parquet":
            events.to_parquet(file_events,use_deprecated_int96_timestamps=True)
            saved = True
        
        if not saved:
            print(f"suffix {suffix}: format not recognized")

        if saveOnly:
            return saved
    return events



def convert_to_ny_timezone(dt):
    """ Assuming the original datetime is in UTC, make it timezone aware
    and convert it to 'America/New_York' timezone."""
    utc_dt = pytz.utc.localize(dt)
    # Convert to 'America/New_York' timezone
    ny_dt = utc_dt.astimezone(pytz.timezone('America/New_York'))
    return ny_dt


### Function to draw data
def plotYvsX(x,y ,xlabel,ylabel,title,saving_dir) :
    """ Function to plot the trade price vs time.\n
    Parameters
    ----------
    filtered_index_series : (list) The list of filtered index series.\n
    filtered_df['trade_price'].values : (list) The list of trade price values.\n
    'Time (America/New_York timezone)' : (str) The time in America/New_York timezone.\n
    'Trade price' : (str) The trade price.\n
    f'AAPL trade price in the flash crash day ' : (str) The title of the plot.\n
    plots_dir : (str) The directory where the plots are saved.\n
    Returns
    -------
    None"""

    # plt.figure(figsize=(20, 6))
    plt.plot(x, y)
    plt.xlabel(xlabel, fontsize=16)
    plt.ylabel(ylabel, fontsize=16)
    plt.title(title, fontsize=20)
    if not os.path.exists(saving_dir):
        os.makedirs(saving_dir)
    plt.savefig(os.path.join(saving_dir, title + '.png'))
    plt.grid()
    plt.show()


############################################################################################################################################
############ Response function Helpers #####################################################################################################
############################################################################################################################################
    # Define a function to apply the Lee-Ready logic for each row

def classify_trade(row):
    if pd.isna(row['trade_price']):
        return np.nan
    previous_mid_price = row['prev_mid_price']
    
    if row['trade_price'] > previous_mid_price:
        return 1  ## Buyer side 
    elif row['trade_price'] < previous_mid_price:
        return -1 ## Seller side 
    else:
        if row['trade_price'] > row['prev_trade']:
            return 1 
        else :
            return -1
        

                 
def tradeclass(i,df,j=None):
    """ Compute the trade class for each trade. Classify each trade as a buyer-initiated trade, a seller-initiated trade, or a non-informative trade. Check the previous trade price and the previous mid-price."""
    if j == None:
        if df.at[i, 'trade_price'] > df.at[i, 'mid_price']:
                df.at[i, 'trade_class'] = 1
        elif df.at[i, 'trade_price'] < df.at[i, 'mid_price']:
                df.at[i, 'trade_class'] = -1
        else:
            j = i - 1
            while j >= 0 and pd.isna(df.at[j, 'trade_price']):
                j -= 1
            if j >= 0:
                return tradeclass(i,df,j)
            else:
                df.at[i, 'trade_class'] = 0
    else:
         if df.at[j, 'trade_price'] > df.at[i, 'trade_price']:
             df.at[i, 'trade_class'] = -1
         elif df.at[j, 'trade_price'] < df.at[i, 'trade_price']:
             df.at[i, 'trade_class'] = 1
         else:
             k = j - 1
             while k >= 0 and pd.isna(df.at[k, 'trade_price']):
                 k -= 1
             if k >= 0:
                 return(tradeclass(i,df,k))
             else:
                 df.at[i, 'trade_class'] = 0 


def setup_response_function_data(df):
    """ Setup the data for the response function analysis. 
    If not found,Compute the mid-price, previous mid-price,pervious trade price and the trade class.
    """
    if not isinstance(df, pd.DataFrame): df = df.to_pandas_df()
    assert set( ['trade_price', 'trade_volume', 'bid-price', 'bid-volume', 'ask-price', 'ask-volume'] ).issubset(set(df.columns.tolist()))
    # Fill the missing values in the bid and ask price 
    df.loc[:, ['bid-price','ask-price']] = df[['bid-price','ask-price']].ffill()
    # Compute the mid price
    if 'mid_price' not in df.columns: df['mid_price'] = (df['ask-price'] + df['bid-price']) / 2
    df.reset_index(inplace=True)
    
    if 'trade_class' not in df.columns: 
        df['trade_class'] = np.nan
        for i in range(len(df)):
            if not pd.isna(df.at[i, 'trade_price']):
                tradeclass(i,df) 
    df.dropna(subset=["trade_class"] , inplace=True)

    # Set the index to be the timestamp
    if type(df.index) != pd.DatetimeIndex: 
        if 'index' in df.columns :
            df.set_index('index',inplace=True)
        else:
            df.set_index('ny_index',inplace=True)
    # Convert the index to New York timezone
    if str(df.index.tzinfo) != 'America/New_York':
        ny_index = pd.DatetimeIndex(df['index']) if 'index' in df.columns else pd.DatetimeIndex(df.index)
        ny_index = ny_index.tz_localize('UTC').tz_convert('America/New_York')
        df.loc[:, 'index'] = ny_index
        df.set_index('index',inplace=True)
    return df



def compute_response(df, tau_max=100):
    """ Compute the average response function R(tau) for a given tau."""
    R = []
    R_std = []
    for tau in range(1, tau_max):
        mid_price_tau = df['mid_price'].shift(-tau)
        r_values = df['trade_class'] * ((mid_price_tau - df['mid_price']))
        R.append(np.nanmean(r_values))
        R_std.append(np.nanstd(r_values))
    return R,R_std


def plot_response(R,ticker_name='',confidence_interval=False):
    R, R_std = R
    plt.figure(figsize=(7, 4))
    plt.plot(R)
    if confidence_interval:
        plt.fill_between(range(len(R)), np.array(R) - np.array(R_std), np.array(R) + np.array(R_std), alpha=0.2)
    plt.xlabel('Lags')
    plt.ylabel('Response function')
    plt.title(ticker_name + ' Average response function' )
    plt.grid(True)
    plt.show()


def plot_response_function(df, tau_max =1000, ticker = "",confidence_interval=False):
    df = setup_response_function_data(df)
    R = compute_response_parallel(df, tau_max)
    plot_response(R,ticker_name=ticker,confidence_interval=confidence_interval)




def plot_3day_response_functions(df, tau_max=1000, ticker="", start_date='2010-05-05'):
    end_date = pd.to_datetime(start_date) + pd.DateOffset(days=3)
    end_date = end_date.strftime('%Y-%m-%d')
    df_filtered = df.copy()
    df_filtered = df_filtered[(df_filtered.index >= start_date) & (df_filtered.index <= end_date)]
    df_filtered = setup_response_function_data(df_filtered)
    _, ax = plt.subplots(1, 3, figsize=(20, 6))
    for i in range(3):
        start = pd.to_datetime(start_date) + pd.DateOffset(days=i)
        end = pd.to_datetime(start) + pd.DateOffset(days=1)
        start = start.strftime('%Y-%m-%d')
        end = end.strftime('%Y-%m-%d')
        df_day = df_filtered.copy()
        df_day = df_day[(df_day.index >= start) & (df_day.index <= end)]
        R = compute_response_c(df_day, tau_max)

        # Fit OLS/linear regression on R
        X = np.arange(len(R)).reshape(-1, 1)
        X = sm.add_constant(X)
        model = sm.OLS(R, X)
        results = model.fit()
        predictions = results.predict(X)

        ax[i].plot(R)
        ax[i].plot(predictions, linestyle='--', color='red')
        ax[i].set_xlabel('Tau')
        ax[i].set_ylabel('R(tau)')
        ax[i].set_title(ticker + ' Average Response Function' + f' {start}')
        ax[i].grid()
    plt.tight_layout()
    plt.show()

def plot_3day_response_functions_split(df, tau_max =1000, ticker = "",start_date='2010-05-05'):
    end_date = pd.to_datetime(start_date) + pd.DateOffset(days=3)
    end_date = end_date.strftime('%Y-%m-%d')
    df_filtered = df.copy()
    df_filtered = df_filtered[(df_filtered.index >= start_date) & (df_filtered.index <= end_date)]
    df_filtered = setup_response_function_data(df_filtered)
    fig,ax = plt.subplots(1, 3, figsize=(22, 8))
    for i in range(3):
        start = pd.to_datetime(start_date) + pd.DateOffset(days=i)
        end = pd.to_datetime(start) + pd.DateOffset(days=1)
        start = start.strftime('%Y-%m-%d')
        end = end.strftime('%Y-%m-%d')
        
        df_day = df_filtered.copy()
        df_day = df_day[(df_day.index >= start) & (df_day.index <= end)]

        # Group the DataFrame by hour and compute autocorrelation for each group
        for trade_class in df_day['trade_class'].unique():
            df_hour = df_day[df_day['trade_class'] == trade_class]
            df_hour_clean = df_hour.dropna(subset=['trade_class'])
            R = compute_response_c(df_hour_clean, tau_max)
            ax[i].plot(R, label=f'Trade Class {trade_class}')
        ax[i].legend()
        ax[i].set_xlabel('Tau')
        ax[i].set_ylabel('R(tau)')
        ax[i].set_title(ticker + ' Average Response Function' + f' {start}')
        ax[i].grid()
    plt.tight_layout()
    plt.show()

def plot_3day_response_functions_hourly(df, tau_max =1000, ticker = "",start_date='2010-05-05'):
    end_date = pd.to_datetime(start_date) + pd.DateOffset(days=3)
    end_date = end_date.strftime('%Y-%m-%d')
    df_filtered = df.copy()
    df_filtered = df_filtered[(df_filtered.index >= start_date) & (df_filtered.index <= end_date)]
    
    df_filtered = setup_response_function_data(df_filtered)
    fig,ax = plt.subplots(1, 3, figsize=(22, 8))
    for i in range(3):
        start = pd.to_datetime(start_date,utc=True) + pd.DateOffset(days=i)
        end = pd.to_datetime(start,utc=True) + pd.DateOffset(days=1)
        start = start.strftime('%Y-%m-%d')
        end = end.strftime('%Y-%m-%d')
        
        df_day = df_filtered.copy()
        df_day = df_day[(df_day.index >= start) & (df_day.index <= end)]
        
        df_day['hour'] = pd.DatetimeIndex(df_day.index).hour

        # Group the DataFrame by hour and compute autocorrelation for each group
        for hour in df_day['hour'].unique():
            df_hour = df_day[df_day['hour'] == hour]
            df_hour_clean = df_hour.dropna(subset=['trade_class'])
            # Compute the average response function R(tau)
            R  = compute_response_c(df_hour_clean, tau_max)
            ax[i].plot(R, label=f'Hour {hour}')
        ax[i].legend()
        ax[i].set_xlabel('Tau')
        ax[i].set_ylabel('R(tau)')
        ax[i].set_title(ticker + ' Average Response Function' + f' {start}')
        ax[i].grid()
    plt.tight_layout()
    plt.show()




## using plotly
def plot_3day_response_functions_15min(df, tau_max=1000, ticker="", start_date='2010-05-05'):
    end_date = pd.to_datetime(start_date) + pd.DateOffset(days=3)
    end_date = end_date.strftime('%Y-%m-%d')
    df_filtered = df.copy()
    df_filtered = df_filtered[(df_filtered.index >= start_date) & (df_filtered.index <= end_date)]
    
    df_filtered = setup_response_function_data(df_filtered)
    # Create a Plotly subplot figure with 3 rows and 1 column
    fig = make_subplots(rows=3, cols=1, subplot_titles=[f'{ticker} Response Function {(pd.to_datetime(start_date) + pd.DateOffset(days=i))}' for i in range(3)])
    
    for i in range(3):
        start = pd.to_datetime(start_date).tz_localize('America/New_York') + pd.DateOffset(days=i)
        end = start + pd.DateOffset(days=1)
        df_day = df_filtered[(df_filtered.index >= start) & (df_filtered.index < end)]
        
        # Group the DataFrame by 15-minute intervals
        df_day_grouped = df_day.groupby(pd.Grouper(freq='15T'))
        
        for name, group in df_day_grouped:
            if not group.empty:
                group_clean = group.dropna(subset=['trade_class'])
                # Compute the average response function R(tau) for the group
                R ,R_std = compute_response(group_clean, tau_max)
                # Create a scatter plot for each group and add it to the corresponding subplot
                fig.add_trace(go.Scatter(x=list(range(len(R))), y=R, mode='lines', name=name.strftime('%H:%M')), row=i+1, col=1)
        
        # Update x-axis and y-axis labels
        fig.update_xaxes(title_text='Tau', row=i+1, col=1)
        fig.update_yaxes(title_text='R(tau)', row=i+1, col=1)

    # Update layout and show the figure
    fig.update_layout(height=600, width=1200, title_text=f'{ticker} Response Functions', showlegend=True)
    fig.show()

def compute_response_volume(df, tau=1):
    """ Compute the response function R(V,tau) for a given tau as a function of the volume V."""
    R = []
    df.dropna(subset=['trade_volume'], inplace=True)
    df.dropna(subset=['mid_price'], inplace=True)
    for volume in df.trade_volume.unique():
        # Now, compute the mid-price at time t + tau
        df_volume = df[df.trade_volume == volume]
        # df_volume = df_volume[df_volume['trade_class']==1]
        mid_price_tau = df_volume['mid_price'].shift(-tau)
        # Compute the weighted price difference for each trade
        r_values = df_volume['trade_class'] * ((mid_price_tau - df_volume['mid_price'])**2)
        # Compute the average response function R(tau)
        R_tau = r_values.mean()
        R.append((volume,R_tau))
    return R

def compute_response_tau(tau, df):
    """ Compute the response function R(tau) for a given tau."""
    mid_price_tau = df['mid_price'].shift(-tau)
    r_values = df['trade_class'] * ((mid_price_tau - df['mid_price']))
    return tau, r_values.mean(), r_values.std()

def compute_response_parallel(df, tau_max=100, num_processes=4):
    """ Compute the average response function R(tau) in parallel using multiple processes."""
    tau_values = range(1, tau_max)
    pool = Pool(processes=num_processes)
    results = pool.starmap(compute_response_tau, [(tau, df) for tau in tau_values])
    pool.close()
    pool.join()
    results.sort(key=lambda x: x[0])  # Sort results by tau
    R = [x[1] for x in results]
    R_std = [x[2] for x in results] 
    return R , R_std



def compute_response_c(data, tau=100):
    """ Computes the response function R(tau) for a given tau using the convolution method."""
    data = data.dropna()
    N = len(data["mid_price"])
    tau_max = tau
    tau = N
    idx =  [ N-i for i in range(1,tau)]
    sn_pn_tau = scipy.signal.fftconvolve(data['mid_price'],data["trade_class"][::-1],mode='full')[-(tau-1):]
    sn_pn_convolve = []
    for i in range(1,tau):
        truncated_mid_price = data['mid_price'][:tau - i]
        truncated_trade_class = data['trade_class'][:tau - i]
        convolution_result = scipy.signal.fftconvolve(truncated_mid_price, truncated_trade_class[::-1], mode='valid')
        sn_pn_convolve.append(convolution_result[0])
    R = (sn_pn_tau-sn_pn_convolve)/ idx
    return R[:tau_max-1]

############################################################################################################################################
############ Training Helpers #####################################################################################################
############################################################################################################################################

def check_flash_crash(deviation,threshold):
    if deviation > threshold:
        return True
    else:
        return False
    
def update_metrics(metrics,flash_crash,true_label):
    if flash_crash and true_label : metrics["TP"] += 1
    elif flash_crash and not true_label : metrics["FP"] += 1
    elif not flash_crash and true_label : metrics["FN"] += 1
    elif not flash_crash and not true_label : metrics["TN"] += 1
    return metrics

def f1_score(metrics):
    if (metrics["TP"]+metrics["FP"] == 0) or (metrics["TP"]+metrics["FN"] == 0) : return 1
    precision = metrics["TP"]/(metrics["TP"]+metrics["FP"])
    recall = metrics["TP"]/(metrics["TP"]+metrics["FN"])
    return 2*(precision*recall)/(precision+recall)
def accuracy(metrics):
    return (metrics["TP"]+metrics["TN"])/(metrics["TP"]+metrics["TN"]+metrics["FP"]+metrics["FN"])