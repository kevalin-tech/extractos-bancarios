#!/usr/bin/env python3

# script to extract transactions from santander and revolut and convert to csv
# 1. download latest FX rates from ECB
# 2. update the .csv file with the latest FX rates

import os
import glob
import warnings
import configparser

import requests
import zipfile
import pandas as pd

# Get secret URL API
keys = configparser.ConfigParser()
keys.read("../accounting_config.txt")
dropbox_path = keys.get("extractos", "DROPPATH")


# ignore openpyxl warning
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')
today = pd.Timestamp.now().date()

def download_fx_rates():
    url = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip?f17b39d5facebdac756b8b7fc923edf8"
    response = requests.get(url)
    if response.status_code == 200:
        with open("fx/eur_rates.zip", "wb") as f:
            f.write(response.content)


def unzip_fx_rates():
    with zipfile.ZipFile("fx/eur_rates.zip", "r") as zip_ref:
        zip_ref.extractall("fx")


# when is the last time the rates were updated?
# read the csv file
fx = pd.read_csv("fx/eurofxref-hist.csv")
last_date = pd.to_datetime(fx["Date"].max()).date()
if last_date < pd.Timestamp.now().date():
    download_fx_rates()
    unzip_fx_rates()
else:
    print("FX rates are up to date \n")

# PROCESS REVOLUT

# extractos revolut ya procesados:
last_extracts_revolut = glob.glob('./extractos_procesados/*Revolut.csv') 
# get the file which was last modified
last_extract_revolut = max(last_extracts_revolut, key=os.path.getmtime)
revolut_processed = pd.read_csv(last_extract_revolut)
revolut_processed['Date started (UTC)'] = pd.to_datetime(revolut_processed['Date started (UTC)'])
max_processed_date_revolut = revolut_processed['Date started (UTC)'].max()

# ultimos extractos de revolut:
revolut_files = glob.glob('./ultimos_extractos/transaction-statement*.csv') 
revolut = pd.read_csv(revolut_files[0])
revolut['Date started (UTC)'] = pd.to_datetime(revolut['Date started (UTC)'])
max_last_date_revolut = revolut['Date started (UTC)'].max()

if max_last_date_revolut > max_processed_date_revolut:
    print("New transactions in Revolut")
    # merge revolut and revolut_processed, deduplicate and save to csv
    merged = pd.concat([revolut, revolut_processed])
    merged = merged.drop_duplicates(subset=['Date started (UTC)', 'ID'], keep='first')
    merged = merged.sort_values(by='Date started (UTC)', ascending=False)
    merged['Date started (UTC)'] = merged['Date started (UTC)'].dt.strftime('%Y-%m-%d')
    # save to csv
    merged.to_csv(f"./extractos_procesados/{today}_Transacciones_Revolut_full.csv", index=False)
    # save an excel with only certain columns
    simple_merged = merged[['Date started (UTC)', 'Date completed (UTC)', 
                            'Type', 'Description', 'Reference',
                              'Card number', 
                              'Orig amount', 'Orig currency', 
                              'Amount', 'Payment currency', 
                              'Total amount', 'Exchange rate',
                              'Fee', 'Fee currency', 'Balance',
                              'Account']]

    simple_merged.to_excel(f"./extractos_procesados/{today}_Transacciones_Revolut.xlsx", index=False)
    print("Added latest transactions to Revolut \n")

    # let's add EUR conversion to the csv
    # read the csv file
    fx = pd.read_csv("fx/eurofxref-hist.csv")
    # Convert 'Date started (UTC)' to datetime if it's not already
    merged['Date started (UTC)'] = pd.to_datetime(merged['Date started (UTC)'])

    # Convert fx 'Date' to datetime
    fx['Date'] = pd.to_datetime(fx['Date'])

    # Merge the fx rates with the merged dataframe
    merged = pd.merge_asof(merged.sort_values('Date started (UTC)'),
                           fx[['Date', 'USD']].sort_values('Date'),
                           left_on='Date started (UTC)',
                           right_on='Date',
                           direction='backward')

    # Rename the 'USD' column to 'EUR_USD_Rate'
    merged = merged.rename(columns={'USD': 'EUR_USD_Rate'})

    # Sort the dataframe back to its original order
    merged = merged.sort_values('Date started (UTC)', ascending=False)
    merged['EUR_amount'] = merged.apply(lambda row: round(row['Amount'] / row['EUR_USD_Rate'], 2) \
        if row['Payment currency'] == 'USD' else round(row['Amount'] * row['EUR_USD_Rate'], 2), axis=1)
    merged['EUR_fee'] = merged.apply(lambda row: round(row['Fee'] / row['EUR_USD_Rate'], 2) 
                                         if row['Fee currency'] == 'USD' else row['Fee'], axis=1)
    # Convert 'Date started (UTC)' back to string format
    merged['Date started (UTC)'] = merged['Date started (UTC)'].dt.strftime('%Y-%m-%d')

    # Update the CSV and Excel files with the new EUR_USD_Rate column
    merged.to_csv(f"./extractos_procesados/{today}_Transacciones_Revolut_full_EUR.csv", index=False)
    simple_merged = merged[['Date started (UTC)', 'Date completed (UTC)', 
                            'Type', 'Description', 'Reference',
                            'Card number', 
                            'Orig amount', 'Orig currency', 
                            'Amount', 'Payment currency', 
                            'Total amount', 'Exchange rate',
                            'Fee', 'Fee currency', 'Balance',
                            'Account', 'EUR_USD_Rate',
                            'EUR_amount', 'EUR_fee']]
    simple_merged.to_excel(f"./extractos_procesados/{today}_Transacciones_Revolut_EUR.xlsx", index=False)

    print("Added EUR_USD_Rate to Revolut transactions \n")
else:
    print("No new transactions in Revolut \n")



# PROCESS SANTANDER

last_extracts = glob.glob('./extractos_procesados/*Santander.xlsx') 
# get the file which was last modified
last_extract = max(last_extracts, key=os.path.getmtime)

# extractos ya procesados:
santander = pd.read_excel(last_extract)
santander['Fecha Operación'] = pd.to_datetime(santander['Fecha Operación']).dt.date
santander['Fecha Valor'] = pd.to_datetime(santander['Fecha Valor']).dt.date
max_processed_date = santander['Fecha Operación'].max()

# ultimos extractos de santander:
last_santander = pd.read_excel("ultimos_extractos/MovimientosCuenta.xlsx", skiprows=7)
last_santander['Fecha Operación'] = pd.to_datetime(last_santander['Fecha Operación'], dayfirst=True).dt.date
last_santander['Fecha Valor'] = pd.to_datetime(last_santander['Fecha Valor'], dayfirst=True).dt.date
max_last_date = last_santander['Fecha Operación'].max()

if max_last_date > max_processed_date:
    print("New transactions in Santander")
    # merge santande and last_santander, deduplicate and save to xlsx
    santander_merged = pd.concat([santander, last_santander])
    santander_merged = santander_merged.drop_duplicates(subset=['Fecha Operación', 'Concepto', 'Importe'], keep='first')
    santander_merged = santander_merged.sort_values(by='Fecha Operación', ascending=False)

    # Convert Fechas to datetime if it's not already
    santander_merged['Fecha Operación'] = pd.to_datetime(santander_merged['Fecha Operación'])
    santander_merged['Fecha Valor'] = pd.to_datetime(santander_merged['Fecha Valor'])

    santander_merged['Fecha Operación'] = santander_merged['Fecha Operación'].dt.strftime('%Y-%m-%d')
    santander_merged['Fecha Valor'] = santander_merged['Fecha Valor'].dt.strftime('%Y-%m-%d')

    # save with today's date
    santander_merged.to_excel(f"./extractos_procesados/{today}_Transacciones_Santander.xlsx", index=False)
    print("Added latest transactions to Santander \n")
else:
    print("No new transactions in Santander")


# Copy to Dropbox
os.system(f"cp ./extractos_procesados/{today}_Transacciones_Santander.xlsx {dropbox_path}/")
os.system(f"cp ./extractos_procesados/{today}_Transacciones_Revolut_EUR.xlsx {dropbox_path}/")
os.system(f"cp ./extractos_procesados/{today}_Transacciones_Revolut_full_EUR.csv {dropbox_path}/")


