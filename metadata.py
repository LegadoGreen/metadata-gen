import os
import random
import json
from datetime import datetime, timedelta
import geopandas as gpd
from shapely.geometry import Point
import urllib.request
import zipfile
import io
from io import StringIO

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

def simulate_weather_data(lat, lon, date_str):
    """
    Simulate weather data with realistic values based on latitude and season.
    """
    # Calculate season based on date
    date = datetime.strptime(date_str, "%Y-%m-%d")
    month = date.month
    season = (month % 12 + 3) // 3  # 1=Winter, 2=Spring, 3=Summer, 4=Fall
    
    # Base temperature varies by latitude and season
    base_temp = 20 - (abs(lat) * 0.5)  # Temperature decreases with latitude
    season_adjustment = {
        1: -10,  # Winter
        2: 5,    # Spring
        3: 15,   # Summer
        4: 0     # Fall
    }
    temperature = base_temp + season_adjustment[season] + random.uniform(-5, 5)
    
    # Precipitation varies by season and latitude
    base_precip = 50 + (abs(lat) * 0.3)  # More precipitation near equator
    precip = base_precip + random.uniform(-20, 20)
    
    # Generate hourly data
    hourly_data = []
    for hour in range(24):
        hourly_data.append({
            "time": f"{date_str}T{hour:02d}:00",
            "temperature_2m": round(temperature + random.uniform(-3, 3), 1),
            "precipitation": round(max(0, precip + random.uniform(-5, 5)), 1),
            "relative_humidity_2m": round(random.uniform(40, 90), 1),
            "wind_speed_10m": round(random.uniform(0, 30), 1),
            "wind_direction_10m": round(random.uniform(0, 360), 1),
            "cloud_cover": round(random.uniform(0, 100), 1),
            "pressure_msl": round(random.uniform(980, 1020), 1)
        })
    
    return {"hourly": hourly_data}

def simulate_environmental_data(lat, lon, date_str):
    """
    Simulate environmental data with realistic values based on location.
    """
    # Elevation data (higher near equator and poles)
    base_elevation = 1000 + (abs(lat) * 50)  # Base elevation increases with latitude
    elevation = base_elevation + random.uniform(-500, 500)
    
    # Air quality (worse in urban areas, better in remote areas)
    air_quality = {
        "pm25": round(random.uniform(5, 50), 1),  # PM2.5 in µg/m³
        "pm10": round(random.uniform(10, 100), 1),  # PM10 in µg/m³
        "no2": round(random.uniform(10, 100), 1),  # NO2 in µg/m³
        "o3": round(random.uniform(20, 150), 1)  # O3 in µg/m³
    }
    
    # Climate data
    climate_data = {
        "annual_precipitation": round(random.uniform(200, 2000), 1),  # mm/year
        "annual_temperature": round(random.uniform(-20, 30), 1),  # °C
        "humidity": round(random.uniform(40, 90), 1)  # %
    }
    
    # Terrain data
    terrain_data = {
        "slope": round(random.uniform(0, 45), 1),  # degrees
        "aspect": round(random.uniform(0, 360), 1),  # degrees
        "roughness": round(random.uniform(0, 100), 1)  # index
    }
    
    return {
        'elevation_data': {
            'elevation': round(elevation, 1),
            'slope': round(random.uniform(0, 45), 1),
            'aspect': round(random.uniform(0, 360), 1)
        },
        'air_quality_data': air_quality,
        'climate_data': climate_data,
        'terrain_data': terrain_data
    }

def simulate_planetary_computer_data(lat, lon, date_str):
    """
    Simulate satellite and environmental data that would come from Planetary Computer.
    """
    # Generate random but realistic values for various environmental indicators
    return {
        "sentinel-2-l2a": f"https://example.com/sentinel2/{date_str.replace('-', '')}_{lat}_{lon}.jpg",
        "landsat-8-c2-l2": f"https://example.com/landsat8/{date_str.replace('-', '')}_{lat}_{lon}.jpg",
        "ndvi": round(random.uniform(0, 1), 3),  # Normalized Difference Vegetation Index
        "land_surface_temperature": round(random.uniform(-20, 50), 1),  # °C
        "precipitation": round(random.uniform(0, 100), 1),  # mm
        "elevation": round(random.uniform(-100, 5000), 1)  # meters
    }

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

def simulate_imagery(lat, lon, date_str):
    """
    Simulate satellite imagery URL based on coordinates and date.
    """
    return f"https://example.com/satellite/{date_str.replace('-', '')}_{lat}_{lon}.jpg"

# Directory where the metadata files will be stored
output_folder = "metadata_files"
os.makedirs(output_folder, exist_ok=True)

# Initialize GeoJSON file if it doesn't exist
geojson_path = os.path.join(output_folder, "points.geojson")
if not os.path.exists(geojson_path):
    initial_geojson = {
        "type": "FeatureCollection",
        "features": []
    }
    with open(geojson_path, "w") as f:
        json.dump(initial_geojson, f)

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
    
    # Add point to GeoJSON file
    try:
        with open(geojson_path, "r") as f:
            geojson_data = json.load(f)
        
        new_feature = {
            "type": "Feature",
            "properties": {
                "id": str(i),
                "name": f"Legado Point {i}",
                "co2_saved": co2_saved,
                "deforestation_prevented": deforestation_prevented,
                "minted_date": minted_date_str
            },
            "geometry": {
                "type": "Point",
                "coordinates": [lon, lat]  # GeoJSON uses [longitude, latitude] order
            }
        }
        
        geojson_data["features"].append(new_feature)
        
        # Write back to file
        with open(geojson_path, "w") as f:
            json.dump(geojson_data, f)
    except Exception as e:
        print(f"Error updating GeoJSON file: {e}")
    
    # Simulate data instead of making API calls
    weather_data = simulate_weather_data(lat, lon, minted_date_for_api)
    environmental_data = simulate_environmental_data(lat, lon, minted_date_for_api)
    planetary_computer_data = simulate_planetary_computer_data(lat, lon, minted_date_for_api)
    planetary_image = simulate_imagery(lat, lon, minted_date_for_api)
    
    # Build the metadata dictionary with the simulated data
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
            "coordinates": {
                "latitude": lat,
                "longitude": lon
            },
            "world_conditions_on_mint": {
                "co2_ppm": co2_ppm,
                "global_temperature_anomaly_c": global_temp,
                "ch4_ppb": ch4_ppb,
                "arctic_sea_ice_min_extent_million_km2": arctic_ice,
                "ice_sheets_status": "Net Mass Loss",
                "sea_level_mm_above_ref": sea_level,
                "ocean_warming_status": "Elevated",
                "nasa_image": f"https://apod.nasa.gov/apod/calendar/S_{minted_date_for_image}.jpg",
                "planetary_image": planetary_image,
                "weather_data": weather_data,
                "environmental_data": environmental_data,
                "planetary_computer_data": planetary_computer_data,
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
                }
            ],
            "future_updates": {}
        }
    }
    
    # Write metadata to file
    file_path = os.path.join(output_folder, f"{i}.txt")
    with open(file_path, "w") as f:
        json.dump(metadata, f, indent=2)
    
    # Print progress every 100,000 files
    if i % 100000 == 0:
        print(f"Generated {i} files.")

print("Finished generating metadata files and GeoJSON.")
