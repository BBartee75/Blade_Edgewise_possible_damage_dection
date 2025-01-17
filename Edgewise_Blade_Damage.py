import pyodbc
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import concurrent.futures
from sqlalchemy import create_engine

#--------------------------------------------------------------------------------------------------
# Functions needed for the analysis
# -------------------------------------------------------------------------------------------------

# Function to Calculate the wind direction difference
def wind_dir_diff(amb_wind_dir_avg, nac_direction_avg):
    # Input validation
    if not isinstance(amb_wind_dir_avg, (int, float)):
        raise TypeError("amb_wind_dir_avg must be a numeric type")
    if not isinstance(nac_direction_avg, (int, float)):
        raise TypeError("nac_direction_avg must be a numeric type")
    # Calculate absolute difference
    diff = abs(amb_wind_dir_avg - nac_direction_avg)
    # Wrap around 360 degrees if necessary
    wrapped_diff = diff if diff <= 180 else 360 - diff
    return wrapped_diff

# Function to Categorize Yaw_Error values into quartiles
def categorize_angle_quartiles(yaw_error):
    # Input validation
    if not isinstance(yaw_error, (int, float)) or yaw_error < 0:
        raise ValueError("yaw_error must be a non-negative number")
    yaw_error = round(yaw_error)
    # Define quartiles
    quartiles = {
        range(0, 10): 10,
        range(10, 20): 20,
        range(20, 40): 40,
        range(40, 60): 60,
        range(60, 90): 90
    }
    # Find the appropriate quartile
    for angle_range, category in quartiles.items():
        if yaw_error >= min(angle_range) and yaw_error < max(angle_range):
            return category
    return 100

# Function to Categorize Risk based on wind speed and angle quartiles
def categorize_Duration_Risk(windMAX, Angle_Quartile, stall_duration):
    # Input validation
    if not isinstance(windMAX, (int, float)) or windMAX < 0:
        raise ValueError("windMAX must be a non-negative number")
    if not isinstance(Angle_Quartile, (int, float)) or Angle_Quartile < 0:
        raise ValueError("Angle_Quartile must be a non-negative number")
    if not isinstance(stall_duration, int) or stall_duration < 0:
        raise ValueError("stall_duration must be a non-negative integer")
    # Normalize inputs to ensure consistency
    windMAX = max(0, min(windMAX, 15))
    Angle_Quartile = max(0, min(Angle_Quartile, 180))
    # stall_duration = max(0, min(stall_duration, 1440))
    # Order elif statements
    if (windMAX >= 15) or (Angle_Quartile >= 60):
        return 5  # 'high.risk'
    elif (12 <= windMAX < 15) or (40 <= Angle_Quartile < 60):
        return 4  # 'medium.high.risk'
    elif (10 <= windMAX < 12) or (20 <= Angle_Quartile < 40):
        return 3  # 'medium.risk'
    elif (8 <= windMAX < 10) or (10 <= Angle_Quartile < 20):
        return 2  # 'medium.low.risk'
    else:
        raise ValueError("Unexpected combination of inputs")

# Function to Categorize stall duration into hour categories
def categorize_hours(minutes):
    # Input validation
    if not isinstance(minutes, (int, float)) or minutes < 0:
        raise ValueError("minutes must be a non-negative number")
    # Convert to integer
    minutes = int(round(minutes))
    # Calculate total hours from minutes
    hours = minutes // 60
    # Determine category based on hours
    if hours < 4:
        return 4
    elif hours <= 8:
        return 8
    elif hours <= 12:
        return 12
    elif hours <= 16:
        return 16
    elif hours <= 20:
        return 20
    elif hours <= 24:
        return 24
    elif hours <= 36:
        return 36
    else:
        return 48

# Function to fetch data for a specific WTG and date range
def fetch_data(wt, start, end):
    server = 'xxxxxxxx'
    database = 'xxxxxxxDB2'
    connection_string = f'mssql+pyodbc://{server}/{
        database}?driver=ODBC+Driver+17+for+SQL+Server'

    # Query template to fetch data for a specific WTG and date range
    # enter the correct names for you databse and tables
    query_template = f"""
    SELECT
    YEAR(A.PCTimeStamp) as Year,
    MONTH(A.PCTimeStamp) as Month,
    DAY(A.PCTimeStamp) as Day,
    A.PCTimeStamp,
    '{wt}' as WTG, -- Placeholder for WTG value
    A.Amb_Temp_Avg,
    A.Amb_WindSpeed_Max,
    A.Amb_WindSpeed_Avg, 
    A.Amb_WindDir_Abs_Avg,
    A.Nac_Direction_Avg,
    A.Grd_Prod_Pwr_Avg,
    A.Sys_Logs_FirstActAlarmNo,
    A.Blds_PitchAngle_Avg
    FROM [SouthPlains1_CustomerDB2].[dbo].[T_{wt}_AP10MinData] as A
    WHERE
        --A.Grd_Prod_Pwr_Avg < 0
        --AND A.Blds_PitchAngle_Avg > 80
        --AND
        A.Amb_WindSpeed_Avg > 8
        AND A.PCTimeStamp >= '{start}'
        AND A.PCTimeStamp <= '{end}';
    """

    try:
        engine = create_engine(connection_string)
        SPData = pd.read_sql_query(query_template, engine)
        return SPData
    except Exception as e:
        print(f"Error querying table T_{wt}_AP10MinData: {e}")
        return pd.DataFrame()

# Function to Use parallel processing to fetch data for all WTGs and date ranges
def fetch_all_data(Q_tables, tables):
    results = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for index, row in Q_tables.iterrows():
            start = row['Start_Date']
            end = row['End_Date']
            for table_name in tables['TABLE_NAME']:
                wt = table_name.split('_')[1]
                futures.append(executor.submit(fetch_data, wt, start, end))
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())
    return pd.concat(results, ignore_index=True)

# Function to filter a DataFrame to retain only 10-minute intervals in chronological order
# enter the correct names for you databse and tables
def filter_10min_intervals(df):
    """
    Filters a DataFrame to retain only 10-minute intervals in chronological order.    
    """
    # Check if required columns exist
    required_columns = ['WTG', 'PCTimeStamp']
    if not all(col in df.columns for col in required_columns):
        raise ValueError(
            "DataFrame must contain 'WTG' and 'PCTimeStamp' columns")
    # Convert PCTimeStamp to datetime
    df['PCTimeStamp'] = pd.to_datetime(df['PCTimeStamp'])
    # Sort by WTG and PCTimeStamp
    df = df.sort_values(['WTG', 'PCTimeStamp'])
    # Calculate time difference in minutes
    df['TimeDiff'] = df.groupby(
        'WTG')['PCTimeStamp'].diff().dt.total_seconds() / 60
    # Check if time difference is approximately 10 minutes
    df['Is10MinInterval'] = np.isclose(df['TimeDiff'], 10, atol=1)
    # Shift Is10MinInterval column by 1
    df['Is10MinIntervalPrevious'] = df.groupby(
        'WTG')['Is10MinInterval'].shift()
    # Assign 1 if previous row is in datetime order, else 2
    df['InOrder'] = np.where(df['Is10MinIntervalPrevious'], 1, 2)
    # Create a new column to track the start of each 10-minute interval
    df['StartOfInterval'] = np.where(
        (df['InOrder'] == 1) & (df['Is10MinInterval']), True, False)
    # Find the index of the last row in each group
    last_row_index = df.groupby('WTG')['StartOfInterval'].cumsum().idxmax()
    # Calculate the time difference between consecutive 10-minute intervals
    df['TimeDiffBetweenIntervals'] = df.groupby(
        'WTG')['PCTimeStamp'].diff().dt.total_seconds() / 60
    # Filter for rows where the next interval starts immediately after
    df['ValidInterval'] = np.where((df['InOrder'] == 1) &
                                   (df['Is10MinInterval'] == True) &
                                   (df.index.get_level_values(0) < last_row_index),
                                   1, 0)
    # Drop intermediate columns
    columns_to_drop = ['TimeDiff', 'Is10MinInterval',
                       'Is10MinIntervalPrevious', 'InOrder', 'StartOfInterval']
    df = df.drop(columns_to_drop, axis=1)
    # Filter out intervals that are not in chronological order
    df = df[df['ValidInterval'] == 1].reset_index(drop=True)
    return df

# Function to process wind turbine data and calculate various metrics
def process_data(wtg):
    """
    Process wind turbine data and calculate various metrics.    
    """
    print(f"Fetching data for WTG: {wtg}")

    # Fetch data for the specific WTG
    result_df = fetch_data(wtg, '2018-01-01', '2023-09-30 00:00:00')
    # Check if input DataFrame is empty or contains NaN values
    if result_df.empty or result_df.isnull().any().any():
        raise ValueError(
            "Input DataFrame must not be empty and contain no NaN values")

    result_df2 = result_df
    # Filters a DataFrame to retain only 10-minute intervals in chronological order.
    if wtg != 207317:
        result_df2 = filter_10min_intervals(result_df2)
    print(f"Processing Edgewise data for: {wtg}")
    result_df = result_df2
    # ----------------------------------------------------------------------------------------------

    # filter date for customer outages
    Miss_file = r'C:\Users\xxxxxx\Desktop\xx\xxx Outages - xx xx Log.xlsx'
    Miss_dates = pd.read_excel(Miss_file)
    Miss_dates['Start Date & Time:'] = pd.to_datetime(
        Miss_dates['Start Date & Time:'])
    Miss_dates['End Date & Time:'] = pd.to_datetime(
        Miss_dates['End Date & Time:'])
    Miss_dates = Miss_dates.iloc[:, [2, 5, 6]]
    '''
    result_df2['PCTimeStamp'] = pd.to_datetime(result_df2['PCTimeStamp'])
    filtered_Missing = result_df2
    filtered_Missing = filtered_Missing.sort_values('PCTimeStamp')    
    #fitler out all data outages 
    pd.options.mode.chained_assignment = None
    for _, row in Miss_dates.iterrows():
        start_date = row['Start Date & Time:']
        end_date = row['End Date & Time:']        
        # Create date range and round to nearest 10-minute interval
        date_range = pd.date_range(start=start_date, end=end_date, freq='10min').round('10min')        
        # Filter filtered_Missing directly using .loc[] instead of creating an intermediate boolean series
        filtered_Missing = filtered_Missing[~filtered_Missing['PCTimeStamp'].isin(date_range)]
       
    result_df = filtered_Missing
    '''
    # add Event_Type Column----------------------------------------------
    # Sort both dataframes by their timestamp columns
    result_df = result_df.sort_values('PCTimeStamp')
    Miss_dates = Miss_dates.sort_values('Start Date & Time:')
    # Create a function to check if timestamp falls within range

    # Function to Check if timestamp falls within range
    def check_time_range(row):
        mask = (Miss_dates['Start Date & Time:'] <= row['PCTimeStamp']) & \
            (Miss_dates['End Date & Time:'] >= row['PCTimeStamp'])
        if any(mask):
            return Miss_dates.loc[mask.idxmax(), 'Event Type:']
        return None
    # Add events column
    result_df['Event_Type'] = result_df.apply(check_time_range, axis=1)
    result_df['Event_Type'] = result_df['Event_Type'].fillna('none')
    # ----------------------------------------------------------------------------------------------

    result_df = result_df[(result_df['Grd_Prod_Pwr_Avg'] <= 0)]

    # Calculate Yaw_Error using the wind_dir_diff function
    result_df['Yaw_Error'] = result_df.apply(lambda row: wind_dir_diff(
        row['Amb_WindDir_Abs_Avg'], row['Nac_Direction_Avg']), axis=1)
    # Filters a DataFrame to retain only 10-minute intervals in chronological order.
    result_df = filter_10min_intervals(result_df)
    # Check if previous row for Nac_Direction_Avg is within 2 degrees
    result_df['possible_stall'] = result_df.groupby(
        'WTG')['Nac_Direction_Avg'].diff().abs() <= 2
    result_df['possible_stall'] = result_df['possible_stall'].map(
        {True: 'possible_stall', False: ''})
    result_df['possible_stall'] = result_df.groupby(
        'WTG')['possible_stall'].shift(fill_value='')
    result_df.loc[result_df.groupby('WTG').head(
        1).index, 'possible_stall'] = ''
    # Find possible Edgewise condition
    result_df['Edgewise_Poss'] = ((result_df['Yaw_Error'] >= 15) &
                                  result_df['ValidInterval'] & result_df['possible_stall'].eq('possible_stall'))

    # ----------------------------------------------------------------------------------------------
    # Filter rows based on conditions
    # any wtg you want to exclude in the final result
    if wtg in [xxxx]:
        filtered_df = result_df
    else:
        filtered_df = result_df[  # (result_df['Grd_Prod_Pwr_Avg']) &
            (result_df['Edgewise_Poss']) &
            (result_df['Yaw_Error'] >= 15)
        ]

    # Convert 'PCTimeStamp' to datetime if not already
    filtered_df['PCTimeStamp'] = pd.to_datetime(filtered_df['PCTimeStamp'])
    data_10Min = filtered_df

    result = filtered_df.groupby(['Year', 'Month', 'Day', 'Event_Type'])[
        ['WTG', 'TimeDiffBetweenIntervals',
         'Amb_WindSpeed_Max', 'Amb_WindSpeed_Avg',
         'Amb_WindDir_Abs_Avg', 'Nac_Direction_Avg',
         'Grd_Prod_Pwr_Avg', 'Sys_Logs_FirstActAlarmNo',
         'Blds_PitchAngle_Avg', 'Yaw_Error']
    ].agg({
        'WTG': 'first',
        'TimeDiffBetweenIntervals': 'sum',
        'Amb_WindSpeed_Max': 'mean',
        'Amb_WindSpeed_Avg': 'mean',
        'Amb_WindDir_Abs_Avg': 'mean',
        'Nac_Direction_Avg': 'mean',
        'Grd_Prod_Pwr_Avg': 'mean',
        'Sys_Logs_FirstActAlarmNo': 'first',
        'Blds_PitchAngle_Avg': 'mean',
        'Yaw_Error': 'mean'
    })

    # WTGS you want to exclude from the final result
    skipwtg = [1111, 1111, 1111]
    if wtg not in skipwtg:
        result = result[(result['TimeDiffBetweenIntervals']) > 60]

    # Convert the index to a column named "Date"
    result = result.reset_index()
    result.columns.values[-1] = 'Yaw_Error'
    result['Date'] = result['Year'].astype(
        str) + '-' + result['Month'].astype(str) + '-' + result['Day'].astype(str)
    result['Date'] = pd.to_datetime(result['Date'])
    result.drop(['Year', 'Month', 'Day'], axis=1, inplace=True)
    result = result[['Date'] + result.columns.drop('Date').tolist()]
    result = result.rename(
        columns={'TimeDiffBetweenIntervals': 'Continuous_Stall_Duration_Minutes'})

    # Create the new 'Angle_Quartile' column
    result['Angle_Quartile'] = result['Yaw_Error'].apply(
        categorize_angle_quartiles)
    # Create the new 'Hours' column
    result['Stall_Hours'] = result['Continuous_Stall_Duration_Minutes'].apply(
        categorize_hours)
    # Create the new 'Damage_Risk' column
    result['Damage_Risk'] = result.apply(lambda row: categorize_Duration_Risk(row['Amb_WindSpeed_Max'],
                                                                              row['Angle_Quartile'],
                                                                              row['Stall_Hours']), axis=1)
    Edgewise_result = result

    return Edgewise_result, data_10Min


# --------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------
# Fetch and process data
# Enter you WTG numbers here
WTGs = (1111, 1111, 1111, 1111, 1111, 1111)

# Initialize DataFrames to store results
Edgewise = pd.DataFrame()
data10Min = pd.DataFrame()
for iteration_number, wtg in enumerate(WTGs, start=1):
    print(f'Iteration Number: {iteration_number}, WTG: {wtg}')
    Edgewise_result, data_10Min = process_data(wtg)
    # Concatenate results to existing DataFrames
    Edgewise = pd.concat([Edgewise, Edgewise_result], ignore_index=True)
    data10Min = pd.concat([data10Min, data_10Min], ignore_index=True)


# Save the final DataFrame to a CSV file
Edgewise.to_csv(
    'C:/Users/bbartee/Desktop/SP/Edgewise_result_Rev6.csv', index=False)
data10Min.to_csv(
    'C:/Users/bbartee/Desktop/SP/data_10Min6_Rev6.csv', index=False)

print('Done')