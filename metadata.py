import os
import random
import json
from datetime import datetime, timedelta
import requests
import geopandas as gpd
from shapely.geometry import Point
from pystac_client import Client
import urllib.request
import zipfile
import io
from io import StringIO
import rasterio
from rasterio.mask import mask
import numpy as np
from sentinelhub import SHConfig, SentinelHubRequest, DataCollection, MimeType
import folium
from PIL import Image
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def random_date(start, end):
    """
    Generate a random datetime between `start` and `end`.
    """
    delta = end - start
    int_delta = int(delta.total_seconds())
    random_second = random.randint(0, int_delta)
    return start + timedelta(seconds=random_second)

def download_natural_earth_data():
    """
    Uses the local Natural Earth 1:110m Cultural Vectors dataset.
    Returns the path to the extracted data.
    """
    # Create a data directory if it doesn't exist
    data_dir = "natural_earth_data"
    os.makedirs(data_dir, exist_ok=True)
    
    # Use the local zip file
    zip_path = "ne_110m_admin_0_countries.zip"
    
    # Extract the data
    try:
        with zipfile.ZipFile(zip_path) as zip_ref:
            zip_ref.extractall(data_dir)
        
        # Return the path to the shapefile
        return os.path.join(data_dir, "ne_110m_admin_0_countries.shp")
    except Exception as e:
        print(f"Error extracting Natural Earth data: {e}")
        return None

# Download and load the Natural Earth data
natural_earth_path = download_natural_earth_data()
if natural_earth_path:
    world = gpd.read_file(natural_earth_path)
    land = world.unary_union
else:
    raise Exception("Failed to download Natural Earth data")

def get_weather_data(lat, lon, date_str):
    """
    Use multiple weather APIs to get comprehensive weather data.
    """
    weather_data = {}
    
    # Open-Meteo API (no API key needed)
    try:
        url = (
            f"https://api.open-meteo.com/v1/forecast?"
            f"latitude={lat}&longitude={lon}&start_date={date_str}"
            f"&end_date={date_str}&hourly=temperature_2m,precipitation,relative_humidity_2m,"
            f"wind_speed_10m,wind_direction_10m,cloud_cover,pressure_msl"
        )
        response = requests.get(url)
        if response.status_code == 200:
            weather_data['open_meteo'] = response.json().get("hourly", {})
    except Exception as e:
        print(f"Error fetching Open-Meteo data: {e}")
    
    # OpenWeather API
    try:
        api_key = os.getenv('OPENWEATHER_API_KEY')
        if api_key:
            url = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&exclude=minutely,hourly,daily,alerts&appid={api_key}&units=metric"
            response = requests.get(url)
            if response.status_code == 200:
                weather_data['openweather'] = response.json()
    except Exception as e:
        print(f"Error fetching OpenWeather data: {e}")
    
    return weather_data

def get_planetary_computer_data(lat, lon, date_str):
    """
    Get comprehensive data from Microsoft Planetary Computer.
    """
    try:
        catalog = Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")
        datetime_range = date_str + "/" + date_str
        
        # Search for various collections
        collections = [
            "sentinel-2-l2a",  # Optical imagery
            "landsat-8-c2-l2",  # Landsat 8 imagery
            "modis-13Q1-006",  # NDVI
            "modis-11A1-006",  # Land Surface Temperature
            "era5-pds",  # Climate reanalysis
            "cop-dem-glo-30"  # Digital Elevation Model
        ]
        
        results = {}
        for collection in collections:
            try:
                search = catalog.search(
                    intersects={
                        "type": "Point",
                        "coordinates": [lon, lat]
                    },
                    datetime=datetime_range,
                    collections=[collection]
                )
                items = list(search.get_items())
                if items:
                    item = items[0]
                    if 'visual' in item.assets:
                        results[collection] = item.assets['visual'].href
                    else:
                        results[collection] = next(iter(item.assets.values())).href
            except Exception as e:
                print(f"Error fetching {collection} data: {e}")
                continue
        
        return results
    except Exception as e:
        print(f"Error fetching Planetary Computer data: {e}")
        return {}

def get_environmental_data(lat, lon, date_str):
    """
    Get environmental data from various sources.
    """
    try:
        # Get elevation data from OpenTopography
        elevation_url = f"https://portal.opentopography.org/API/globaldem?demtype=SRTMGL3&south={lat-0.1}&north={lat+0.1}&west={lon-0.1}&east={lon+0.1}&outputFormat=json"
        elevation_response = requests.get(elevation_url)
        elevation_data = elevation_response.json() if elevation_response.status_code == 200 else {}
        
        # Get air quality data from OpenAQ
        api_key = os.getenv('OPENAQ_API_KEY')
        if api_key:
            air_quality_url = f"https://api.openaq.org/v2/measurements?coordinates={lat},{lon}&date_from={date_str}&date_to={date_str}"
            headers = {'X-Api-Key': api_key}
            air_quality_response = requests.get(air_quality_url, headers=headers)
            air_quality_data = air_quality_response.json() if air_quality_response.status_code == 200 else {}
        else:
            air_quality_data = {}
        
        # Get climate data from World Bank Climate Change Knowledge Portal
        climate_url = f"https://climateknowledgeportal.worldbank.org/api/v1/country/annualavg/pr/{lat}/{lon}"
        climate_response = requests.get(climate_url)
        climate_data = climate_response.json() if climate_response.status_code == 200 else {}
        
        # Get terrain data from OpenStreetMap
        terrain_url = f"https://api.openstreetmap.org/api/0.6/map?bbox={lon-0.1},{lat-0.1},{lon+0.1},{lat+0.1}"
        terrain_response = requests.get(terrain_url)
        terrain_data = terrain_response.text if terrain_response.status_code == 200 else {}
        
        return {
            'elevation_data': elevation_data,
            'air_quality_data': air_quality_data,
            'climate_data': climate_data,
            'terrain_data': terrain_data
        }
    except Exception as e:
        print(f"Error fetching environmental data: {e}")
        return {}

def get_random_land_coordinate():
    """
    Generates random latitude and longitude coordinates that fall on land.
    """
    while True:
        # Approximate ranges where most land areas are found
        lat = random.uniform(-60, 80)
        lon = random.uniform(-180, 180)
        point = Point(lon, lat)
        if land.contains(point):
            return lat, lon

def get_imagery_from_mpc(lat, lon, minted_date_str):
    """
    Query the Microsoft Planetary Computer API to get imagery for given coordinates and minted date.
    Returns an image URL if available.
    """
    try:
        catalog = Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")
        # Use minted_date_str as both start and end for the search interval
        datetime_range = minted_date_str + "/" + minted_date_str
        search = catalog.search(
            intersects={
                "type": "Point",
                "coordinates": [lon, lat]
            },
            datetime=datetime_range,
            collections=["sentinel-2-l2a"]  # Example collection; adjust as needed
        )
        items = list(search.get_items())
        if items:
            item = items[0]
            # Try to get the 'visual' asset; if not available, grab the first asset
            if 'visual' in item.assets:
                return item.assets['visual'].href
            else:
                return next(iter(item.assets.values())).href
    except Exception as e:
        print(f"Error fetching imagery: {e}")
    return "Not Available"

# Directory where the metadata files will be stored
output_folder = "metadata_files"
os.makedirs(output_folder, exist_ok=True)

# Define the time range for the minted timestamp: from 2024-01-01 to 2025-04-02
start_date = datetime(2024, 1, 1, 0, 0, 0)
end_date = datetime(2025, 4, 2, 23, 59, 59)

# Achievements template to include for the first 50 files and occasionally after
achievements_template = [
    {
        "project_name": "Legado Early - Guania Colombia",
        "description": "Before going sale, Legado has helped communities in the Guania region of Colombia to protect 20,000 hectares of rainforest, preventing deforestation and conserving biodiversity.",
        "current_status": "Up to 8 communities are now involved in the project, with 100% of the land protected from deforestation.",
        "region": "Amazon Rainforest, Colombia",
        "co2_sequestered_estimate_tonnes": 4,
        "deforestation_prevented_km2": 5
    },
    {
        "project_name": "Legado Early - Africa",
        "description": "Legado Africa is a project that aims to give access to clean water to 1 million people in Africa by 2030.",
        "current_status": "The project has already provided clean water to 100,000 people in 2024.",
        "region": "Africa",
        "co2_saved_estimate_tonnes": 1,
        "deforestation_prevented_km2": 1
    }
]

# Total number of files to generate
total_files = 20_000_000

for i in range(1, total_files + 1):
    # Generate random attribute values
    co2_saved = random.randint(20, 400)  # CO2 Saved (tonnes)
    deforestation_prevented = random.randint(0, 55)  # Deforestation Prevented (km^2)
    
    # Generate a random minted timestamp within the specified range
    minted_datetime = random_date(start_date, end_date)
    minted_date_str = minted_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
    minted_date_for_api = minted_datetime.strftime("%Y-%m-%d")
    minted_date_for_image = minted_datetime.strftime("%y%m%d")
    
    # Generate random world conditions based on base values ± specified deltas
    co2_ppm = round(420.5 + random.uniform(-50, 50), 2)
    global_temp = round(1.24 + random.uniform(-0.3, 0.3), 2)
    ch4_ppb = round(1895 + random.uniform(-300, 300), 2)
    arctic_ice = round(3.9 + random.uniform(-0.5, 0.5), 2)
    sea_level = round(95 + random.uniform(-10, 10), 2)
    
    # Determine achievements: always include for the first 50, then with a 1% chance.
    if i <= 50 or random.random() < 0.01:
        achievements = achievements_template
    else:
        achievements = []
    
    # Get random land coordinates
    lat, lon = get_random_land_coordinate()
    
    # Fetch imagery from Microsoft Planetary Computer using the minted date and coordinates
    planetary_image = get_imagery_from_mpc(lat, lon, minted_date_for_api)
    
    # Fetch weather data using Open-Meteo API
    weather_data = get_weather_data(lat, lon, minted_date_for_api)
    
    # Fetch planetary computer data
    planetary_computer_data = get_planetary_computer_data(lat, lon, minted_date_for_api)
    
    # Fetch environmental data
    environmental_data = get_environmental_data(lat, lon, minted_date_for_api)
    
    # Build the metadata dictionary with the new world conditions data
    metadata = {
        "attributes": [
            {
                "trait_type": "CO2 Saved (tonnes)",
                "value": co2_saved
            },
            {
                "trait_type": "Deforestation Prevented (km^2)",
                "value": deforestation_prevented
            }
        ],
        "metadata_version": "1.0",
        "token_details": {
            "timestamp_minted": minted_date_str,
            "serial_number": i,
            "world_conditions_on_mint": {
                "co2_ppm": co2_ppm,
                "global_temperature_anomaly_c": global_temp,
                "ch4_ppb": ch4_ppb,
                "arctic_sea_ice_min_extent_million_km2": arctic_ice,
                "ice_sheets_status": "Net Mass Loss",
                "sea_level_mm_above_ref": sea_level,
                "ocean_warming_status": "Elevated",
                "nasa_image": f"https://apod.nasa.gov/apod/calendar/S_{minted_date_for_image}.jpg",
                "coordinates": {
                    "latitude": lat,
                    "longitude": lon
                },
                "planetary_image": planetary_image,
                "weather_data": weather_data,
                "environmental_data": environmental_data,
                "planetary_computer_data": planetary_computer_data,
                "data_sources": [
                    {
                        "name": "NASA GISS",
                        "url": "https://data.giss.nasa.gov/"
                    },
                    {
                        "name": "NOAA Climate Data",
                        "url": "https://www.ncdc.noaa.gov/"
                    },
                    {
                        "name": "Microsoft Planetary Computer",
                        "url": "https://planetarycomputer.microsoft.com/"
                    },
                    {
                        "name": "Open-Meteo",
                        "url": "https://open-meteo.com/"
                    },
                    {
                        "name": "OpenTopography",
                        "url": "https://opentopography.org/"
                    },
                    {
                        "name": "World Bank Climate Change Knowledge Portal",
                        "url": "https://climateknowledgeportal.worldbank.org/"
                    },
                    {
                        "name": "FAO SoilGrids",
                        "url": "https://www.isric.org/explore/soilgrids"
                    }
                ]
            },
            "achievements": achievements,
            "data_sources": [
                {
                    "name": "NASA GISS",
                    "url": "https://data.giss.nasa.gov/"
                },
                {
                    "name": "NOAA Climate Data",
                    "url": "https://www.ncdc.noaa.gov/"
                },
                {
                    "name": "Microsoft Planetary Computer",
                    "url": "https://planetarycomputer.microsoft.com/"
                },
                {
                    "name": "Open-Meteo",
                    "url": "https://open-meteo.com/"
                },
                {
                    "name": "OpenTopography",
                    "url": "https://opentopography.org/"
                },
                {
                    "name": "World Bank Climate Change Knowledge Portal",
                    "url": "https://climateknowledgeportal.worldbank.org/"
                },
                {
                    "name": "FAO SoilGrids",
                    "url": "https://www.isric.org/explore/soilgrids"
                }
            ],
            "future_updates": {}
        }
    }
    
    # Write metadata to file: e.g., metadata_files/1.txt, metadata_files/2.txt, etc.
    file_path = os.path.join(output_folder, f"{i}.txt")
    with open(file_path, "w") as f:
        json.dump(metadata, f, indent=2)
    
    # Optionally, print progress every 100,000 files
    if i % 100000 == 0:
        print(f"Generated {i} files.")

print("Finished generating metadata files.")
