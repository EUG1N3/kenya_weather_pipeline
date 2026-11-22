import sys
import json
import requests
import config
from datetime import date, timedelta

def extraction_geodata(town):
    params = {"name": town, "count": 1, "countryCode": config.COUNTRY_CODE}
    try:  
        response = requests.get(config.GEO_URL, params=params, timeout=config.REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.json()
    
    except requests.exceptions.Timeout:
        print("Server Took Long to Respond")
        sys.exit(1)
    except requests.exceptions.ConnectionError:
        print("Poor Internet Connection")
        sys.exit(1)
    except requests.exceptions.HTTPError as e:
        print(f"Bad Status Code: {e}")
        sys.exit(1)
    except requests.exceptions.JSONDecodeError:
        print("Invalid JSON format!")
        sys.exit(1)

def extraction_weather(latitude, longitude):
    # call weather history for one location
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": config.START_DATE.isoformat(),
        "end_date": (date.today() - timedelta(days=config.DELAY_DAYS)).isoformat(),
        "daily": ",".join(config.DAILY_VARIABLES),
        "timezone": config.TIMEZONE
    }
    try:
        response = requests.get(config.HISTORY_URL, params=params, timeout=config.REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.Timeout:
        print("Weather: Server Took long to respond")
        sys.exit(1)
    except requests.exceptions.ConnectionError:
        print("Weather: Poor Internet Connection")
        sys.exit(1)
    except requests.exceptions.HTTPError as e:
        print(f"Weather: Bad status code: {e}")
        sys.exit(1)
    except requests.exceptions.JSONDecodeError:
        print(f"Weather: Invalid JSON format")
        sys.exit(1)


def main():
    towns = ["Nairobi", "Mombasa", "Kisumu", "Nakuru", "Eldoret"]
    weather_results = {}

    for town in towns:
        print(f"Extracting geodata for {town}....")

        geo_data = extraction_geodata(town)
        if not geo_data.get("results"):  # Fixed: check for 'results' key
            print(f"No geodata found for {town}")  # Fixed: added 'f'
            continue

        # print(geo_data)

        location = geo_data["results"][0]
        lat = location["latitude"]
        lon = location["longitude"]

        print(f"Exctracting weather data for {town}...")

        # get weather data 
        weather_data = extraction_weather(lat, lon)  # Fixed: 'extraction_weather'
        weather_results[town] = {
            "geodata": location,
            "weather": weather_data
        }


    # Save the full response to a JSON file in your project folder
    with open("full_weather_data.json", "w", encoding="utf-8") as f:
        json.dump(weather_results, f, indent=2)
    print()
    print("Saved full weather data to full_weather_data.json!")

    # print(weather_data.keys())
  


if __name__ == "__main__":
    main()
