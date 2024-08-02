from flask import Flask, jsonify, request
import requests
import pandas as pd
import logging
import os
import os
from dotenv import load_dotenv
app = Flask(__name__)

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global variable to store merged GeoJSON data
merged_geojson = None

def fetch_wordpress_data(wp_json_url, per_page=100):
    """
    Fetch WordPress data with pagination.

    :param wp_json_url: URL to fetch WordPress data.
    :param per_page: Number of records per page.
    :return: DataFrame containing WordPress data.
    """
    page = 1
    all_wp_data = []

    while True:
        try:
            response = requests.get(wp_json_url, params={'per_page': per_page, 'page': page})
            response.raise_for_status()  # Raise HTTPError for bad responses
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            break

        data = response.json()

        if not data:
            break

        all_wp_data.extend(data)

        if len(data) < per_page:
            break  # Stop if less than `per_page` records are returned

        page += 1

    wp_df = pd.json_normalize(all_wp_data)
    logger.debug(f"WordPress DataFrame columns: {wp_df.columns.tolist()}")
    return wp_df

def fetch_geojson_data(geojson_url):
    """
    Fetch GeoJSON data.

    :param geojson_url: URL to fetch GeoJSON data.
    :return: DataFrame containing GeoJSON data.
    """
    try:
        response = requests.get(geojson_url)
        response.raise_for_status()
        geojson_data = response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        return pd.DataFrame()

    if 'features' not in geojson_data:
        logger.error("No 'features' key in GeoJSON response")
        return pd.DataFrame()

    geojson_features = geojson_data['features']
    geojson_df = pd.json_normalize([{
        'ADM0_NAME': feature['properties']['ADM0_NAME'],
        'ISO_3_CODE': feature['properties']['ISO_3_CODE'],
        'CENTER_LAT': feature['properties']['CENTER_LAT'],
        'CENTER_LON': feature['properties']['CENTER_LON'],
        "TYPE": feature['geometry']['type'],
        "COORDINATES": feature['geometry']['coordinates']
    } for feature in geojson_features])
    logger.debug(f"GeoDataFrame: {geojson_df.head()}")
    return geojson_df

def merge_data(wp_df, geojson_df):
    """
    Merge WordPress and GeoJSON data on ISO3 codes.

    :param wp_df: DataFrame containing WordPress data.
    :param geojson_df: DataFrame containing GeoJSON data.
    :return: Merged DataFrame.
    """
    wp_df['iso_3_code'] = wp_df['iso_3_code'].str.strip().str.upper()
    geojson_df['ISO_3_CODE'] = geojson_df['ISO_3_CODE'].str.strip().str.upper()

    merged_df = wp_df.merge(geojson_df, left_on='iso_3_code', right_on='ISO_3_CODE', how='inner')
    logger.debug(f"Merged DataFrame: {merged_df.head()}")

    if not (merged_df['ISO_3_CODE'] == 'AFG').any():
        logger.warning("Afghanistan (ISO_3_CODE: AFG) is missing in the merged data.")
    else:
        logger.debug("Afghanistan (ISO_3_CODE: AFG) is present in the merged data.")

    return merged_df

def convert_to_geojson(merged_df):
    """
    Convert merged DataFrame to GeoJSON format.

    :param merged_df: Merged DataFrame.
    :return: GeoJSON dictionary.
    """
    features = []
    for _, row in merged_df.iterrows():
        properties = row.drop(['CENTER_LAT', 'CENTER_LON', 'ISO_3_CODE', 'TYPE', 'COORDINATES']).to_dict()
        geometry = {
            "type": row['TYPE'],
            "coordinates": row['COORDINATES']
        }
        feature = {
            "type": "Feature",
            "geometry": geometry,
            "properties": properties
        }
        features.append(feature)

    geojson = {
        "type": "FeatureCollection",
        "crs": {
            "type": "name",
            "properties": {
                "name": "EPSG:4326"
            }
        },
        "features": features
    }
    return geojson

def fetch_and_merge_data():
    """
    Fetch and merge WordPress and GeoJSON data, and convert to GeoJSON format.
    """
    global merged_geojson

    wp_json_url = os.getenv("WP_JSON_URL", "https://revamp.gpei.acw.website/wp-json/wp/v2/country")
    geojson_url = os.getenv("GEOJSON_URL", "https://services.arcgis.com/5T5nSi527N4F7luB/arcgis/rest/services/Detailed_Boundary_ADM0/FeatureServer/0/query?where=1%3D1&outFields=ADM0_NAME,ISO_3_CODE,CENTER_LAT,CENTER_LON&outSR=4326&f=geojson")

    wp_df = fetch_wordpress_data(wp_json_url)
    geojson_df = fetch_geojson_data(geojson_url)

    if wp_df.empty or geojson_df.empty:
        logger.error("One of the data sources is empty. Cannot proceed with merging.")
        return

    merged_df = merge_data(wp_df, geojson_df)
    merged_geojson = convert_to_geojson(merged_df)
    logger.debug(f"Merged GeoJSON: {str(merged_geojson)[:100]}")  # Print first 100 characters for debug

@app.route('/polio', methods=['GET'])
def get_polio_data():
    """
    Endpoint to get polio data.
    """
    global merged_geojson
    if merged_geojson is None:
        fetch_and_merge_data()
    return jsonify(merged_geojson)

@app.route('/polio/update', methods=['POST'])
def update_polio_data():
    """
    Endpoint to update polio data.
    """
    fetch_and_merge_data()
    return jsonify({"message": "Data updated successfully"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv("PORT", 8001)))

