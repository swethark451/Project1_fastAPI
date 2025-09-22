from fastapi import FastAPI, HTTPException
from dotenv import load_dotenv
import httpx,requests
import os

app = FastAPI()
load_dotenv()

GEO_URL = "http://api.openweathermap.org/geo/1.0/direct"
WEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"
Github_URL = "https://api.github.com/users/"

API_KEY = os.getenv("OPENWEATHER_API_KEY")
if not API_KEY:
    raise RuntimeError("Please set expo in your environment")

@app.get("/get_weather/{city}")
async def get_weather(city: str):
    """
    Path Parameter: /get_weather/{city}
    Example: /get_weather/London

    Steps:
      1. Get coordinates for city
      2. Get weather using lat/lon
    """

    # 1. Get coordinates for the city
    try:
        async with httpx.AsyncClient() as client:
            geo_resp = await client.get(
                GEO_URL,
                params={"q": city, "appid": API_KEY, "limit": 1}
            )
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"Geo API unreachable: {e}")

    if geo_resp.status_code != 200:
        raise HTTPException(status_code=geo_resp.status_code, detail="Error fetching coordinates")

    geo_data = geo_resp.json()
    if not geo_data:  # empty list = invalid city
        raise HTTPException(status_code=404, detail="Invalid city name")

    lat = geo_data[0]["lat"]
    lon = geo_data[0]["lon"]

    # 2. Get weather using coordinates
    try:
        async with httpx.AsyncClient() as client:
            weather_resp = await client.get(
                WEATHER_URL,
                params={"lat": lat, "lon": lon, "appid": API_KEY, "units": "metric"}
            )
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"Weather API unreachable: {e}")

    if weather_resp.status_code != 200:
        raise HTTPException(status_code=weather_resp.status_code, detail="Error fetching weather data")

    weather_data = weather_resp.json()

    # Extract the required fields
    return {
        "city": city,
        "temperature": weather_data["main"]["temp"],  # Celsius
        "weather_description": weather_data["weather"][0]["description"],
    }
@app.get("/get_github_user")

def get_github_user(username: str ):
    try :
        response = requests.get(f"{Github_URL}{username}")
        if response.status_code == 200:
            #print(response.status_code)
            data = response.json()
        elif response.status_code == 403:
            raise HTTPException(status_code=403, detail="API rate limit exceeded")
        elif response.status_code == 404:
            raise HTTPException(status_code=404, detail="User not found")

        #print(data.get("login"))
        return {
            "login": data.get("login"),
            "name": data.get("name"),
            "public_repos": data.get("public_repos"),
            "followers": data.get("followers"),
            "following": data.get("following")
        }
    except requests.RequestException as e:
        raise HTTPException(status_code=500, detail=f"External API request failed: {str(e)}")

