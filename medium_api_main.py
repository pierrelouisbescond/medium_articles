import pandas as pd
import requests
from datetime import datetime
import logging

from google.cloud import bigquery


def hello_pubsub(event, context):

  logging.basicConfig(level=logging.INFO)

  client = bigquery.Client()
  table_id = "api-weather-test-372410.WEATHER.TEMPERATURES"

  logging.info("Connected to Google Cloud Platform Account.")

  def get_temperature_history(
      lat: float,
      lon: float,
      start_date: str,
      end_date: str) -> pd.DataFrame:
      """Get the temperature history for a given site

      Parameters
      ----------
      lat: float
          Latitude of the site
      lon: float
          Longitude of the site
      start_date: str
          Date from which to get the temperature history
      end_date: str
          End date from which to get the temperature history

      Returns
      -------
      pd.DataFrame
          DataFrame with the temperature history
      """    
      API_ENDPOINT = f"https://archive-api.open-meteo.com/v1/era5?latitude={lat}&longitude={lon}&start_date={start_date}&end_date={end_date}&hourly=temperature_2m"

      try:
          response = requests.get(API_ENDPOINT, timeout=5)
      except requests.exceptions.ReadTimeout as err:
          return logging.error(err)
      response_json = response.json()

      hourly_temperatures = pd.DataFrame.from_records(response_json["hourly"])

      hourly_temperatures["time"] = pd.to_datetime(hourly_temperatures["time"])

      hourly_temperatures.set_index("time", inplace=True)

      logging.info(f"Got {len(hourly_temperatures)} hourly temperatures.")
      
      return hourly_temperatures

  def get_temperature_history_all_sites(
      origin_date: str,
      end_date: str):
      """Get the temperature history for all the sites in the DataFrame

      Parameters
      ----------
      sites_df: pd.DataFrame
          DataFrame with the coordinates of the sites
      origin_date: str
          Date from which to get the temperature history
      end_date: str
          Date until which to get the temperature history
      """

      logging.info(f"Getting temperature history for all sites from {origin_date} to {end_date}...")

      sites_coordinates = {"Beinheim": (48.85332,8.092372),
                          "Benifaio": (39.28439,-0.4324797),
                          "Portage_la_Prairie": (49.96306,-98.39294),
                          "Wuhan": (30.69563,114.1483),
                          }

      df_list = []
    
      for site in sites_coordinates:

          col_name = site

          lat, lon = sites_coordinates[site]

          logging.info(f"Getting temperatures for {col_name} at {lat}, {lon}...")

          hourly_temperatures = get_temperature_history(lat, lon, origin_date, end_date)

          hourly_temperatures.rename(columns={"temperature_2m": col_name}, inplace=True)

          df_list.append(hourly_temperatures)

      df_historical = pd.DataFrame()

      df_historical = pd.concat(df_list, axis=1)

      df_historical = df_historical.dropna(axis=0)
      
      return df_historical

  now = datetime.now()
  now_str = now.strftime("%Y-%m-%d")
  origin_str = '2022-01-01'

  df_historical = get_temperature_history_all_sites(origin_str, now_str)

  job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

  job = client.load_table_from_dataframe(df_historical, table_id, job_config=job_config)

  logging.info(f"{len(df_historical)} Records uploaded in {table_id}.")