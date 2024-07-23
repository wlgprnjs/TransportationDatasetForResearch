import pandas as pd
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Directory containing the CSV files
directory = r'C:\Users\wlgpr\UAA\Research\FARS2022NationalCSV'

# Output file for the combined CSV
output_file = r'C:\Users\wlgpr\UAA\Research\2022combindD1D2.csv'

# List of columns to filter
columns_to_filter = ['STATENAME', 'ST_CASE', 'HOURNAME', 'LATITUDENAME', 'LONGITUDNAME', 'WEATHERNAME',
                     'LGT_CONDNAME', 'VSURCONDNAME', 'VISIONNAME', 'PER_NO', 'FATALS', 'VSPD_LIMNAME', 
                     'DRIMPAIR', 'DRIMPAIRNAME', 'TRAV_SPNAME', 'INJ_SEVNAME', 'HARM_EVNAME',]


# List to hold DataFrames
df1 = []
df2 = []

# Loop through each file in the directory
for filename in os.listdir(directory):
    if filename.endswith('.csv'):  # Check if the file is a CSV
        file_path = os.path.join(directory, filename)
        logging.info(f'Processing file: {filename}')
        
        # Try reading the CSV file with iso-8859encoding and low memory option
        try:
            df = pd.read_csv(file_path, encoding= 'iso-8859-1', low_memory=False)
        except UnicodeDecodeError:
            logging.warning(f'UTF-8 encoding failed for {filename}. Trying with latin1 encoding.')
            try:
                df = pd.read_csv(file_path, encoding='latin1', low_memory=False)
            except Exception as e:
                logging.error(f'Failed to read {filename} with latin1 encoding: {e}')
                continue

        # Check if any of the desired columns are in the DataFrame
        available_columns = [col for col in columns_to_filter if col in df.columns]
        if not available_columns:
            logging.warning(f'None of the specified columns are in {filename}. Skipping file.')
            continue
        
        # Filter the DataFrame using the available columns
        new_df = df[available_columns]
        
        # Append the filtered DataFrame to the list
        df1.append(new_df)

# Concatenate all DataFrames in the list
if df1:
    combined_df = pd.concat(df1, ignore_index=True)

    # Sort by 'DRIMPAIR' to prioritize rows with 'DRIMPAIR' information
    combined_df = combined_df.sort_values(by='DRIMPAIR', ascending=True, na_position='last')
    combined_df = combined_df[combined_df['DRIMPAIR'] != 0] # None/Appearently Normal   
    combined_df = combined_df[combined_df['DRIMPAIR'] < 98] # Not reported ( 99 Reported as Unknown if Impaired )
    # Remove duplicate rows based on 'STATENAME' and 'ST_CASE' but keep the first occurrence (which has 'DRIMPAIR' info due to sorting)
    combined_df = combined_df.drop_duplicates(subset=['STATENAME', 'ST_CASE'], keep='first')

   
 
    column_mapping = {
        'STATENAME': 'State',
        'ST_CASE': 'Case Number',
        'HOURNAME': 'Hour',
        'LATITUDENAME': 'Latitude',
        'LONGITUDNAME': 'Longitude',
        'WEATHERNAME': 'Weather Condition',
        'LGT_CONDNAME': 'Light Condition',
        'VSURCONDNAME': 'Surface Condition',
        'VISIONNAME': 'Vision Condition',
        'PER_NO': 'Person Number',
        'FATALS': '# of Fatalities',
        'VSPD_LIMNAME': 'Speed Limit',
        'DRIMPAIR' : 'Impairment #',
        'DRIMPAIRNAME' : 'Impairment Name',
        'TRAV_SPNAME': 'Travel Speed',
        'INJ_SEVNAME': 'Injury Severity',
        'HARM_EVNAME': 'Harm Event'
    }
    
    combined_df = combined_df.rename(columns=column_mapping)
    try:
        # Save the combined data to a single CSV file
        combined_df.to_csv(output_file, index=False)
        logging.info(f'Successfully combined and saved all files to {output_file}')
    except Exception as e:
        logging.error(f'Failed to save the combined CSV file: {e}')
else:
    logging.info('No files were processed.')
