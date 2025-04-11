import os
import json
import multiprocessing
from tqdm import tqdm

def optimize_metadata(data):
    """
    Optimize the metadata structure to reduce file size.
    """
    # Create optimized structure
    optimized = {
        "a": [],  # attributes
        "v": data["metadata_version"],  # version
        "t": {  # token details
            "ts": data["token_details"]["timestamp_minted"],  # timestamp
            "sn": data["token_details"]["serial_number"],  # serial number
            "c": {  # coordinates
                "la": data["token_details"]["coordinates"]["latitude"],  # latitude
                "lo": data["token_details"]["coordinates"]["longitude"]  # longitude
            },
            "w": {  # world conditions
                "cp": round(data["token_details"]["world_conditions_on_mint"]["co2_ppm"], 1),  # co2_ppm
                "gt": round(data["token_details"]["world_conditions_on_mint"]["global_temperature_anomaly_c"], 1),  # global temp
                "ch": round(data["token_details"]["world_conditions_on_mint"]["ch4_ppb"], 1),  # ch4_ppb
                "ai": round(data["token_details"]["world_conditions_on_mint"]["arctic_sea_ice_min_extent_million_km2"], 1),  # arctic ice
                "sl": round(data["token_details"]["world_conditions_on_mint"]["sea_level_mm_above_ref"], 1),  # sea level
                "ni": data["token_details"]["world_conditions_on_mint"]["nasa_image"],  # nasa image
                "pi": data["token_details"]["world_conditions_on_mint"]["planetary_image"],  # planetary image
                "wd": {  # weather data
                    "h": [  # hourly
                        {
                            "t": h["time"],  # time
                            "te": round(h["temperature_2m"], 1),  # temperature
                            "p": round(h["precipitation"], 1),  # precipitation
                            "h": round(h["relative_humidity_2m"], 1),  # humidity
                            "ws": round(h["wind_speed_10m"], 1),  # wind speed
                            "wd": round(h["wind_direction_10m"], 1),  # wind direction
                            "cc": round(h["cloud_cover"], 1),  # cloud cover
                            "pr": round(h["pressure_msl"], 1)  # pressure
                        } for h in data["token_details"]["world_conditions_on_mint"]["weather_data"]["hourly"]
                    ]
                }
            }
        }
    }
    
    # Add achievements if present
    if data["token_details"]["achievements"]:
        optimized["t"]["ac"] = data["token_details"]["achievements"]
    
    return optimized

def minify_file(file_path):
    """
    Minify a single JSON file by removing unnecessary whitespace and optimizing structure.
    """
    try:
        # Read the file
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        # Optimize the data structure
        optimized_data = optimize_metadata(data)
        
        # Write back minified
        with open(file_path, 'w') as f:
            json.dump(optimized_data, f, separators=(',', ':'))
        
        return True
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False

def process_batch(file_paths):
    """
    Process a batch of files.
    """
    success_count = 0
    for file_path in file_paths:
        if minify_file(file_path):
            success_count += 1
    return success_count

def main():
    # Directory containing the metadata files
    input_dir = "metadata_files"
    
    # Get all .txt files
    files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith('.txt')]
    total_files = len(files)
    
    # Number of processes to use (use all available CPU cores)
    num_processes = multiprocessing.cpu_count()
    
    # Split files into batches for parallel processing
    batch_size = (total_files + num_processes - 1) // num_processes
    file_batches = [files[i:i + batch_size] for i in range(0, total_files, batch_size)]
    
    # Create a pool of processes
    pool = multiprocessing.Pool(processes=num_processes)
    
    # Initialize progress bar
    pbar = tqdm(total=total_files, desc="Minifying files")
    
    # Process batches in parallel
    success_count = 0
    for batch_success in pool.imap_unordered(process_batch, file_batches):
        success_count += batch_success
        pbar.update(batch_success)
    
    # Close the pool
    pool.close()
    pool.join()
    pbar.close()
    
    print(f"\nMinification complete!")
    print(f"Successfully processed {success_count} out of {total_files} files")
    print(f"Success rate: {(success_count/total_files)*100:.2f}%")

if __name__ == "__main__":
    main() 