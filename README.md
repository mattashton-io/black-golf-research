# Black Golf Research

A web application to explore and analyze the geographic distribution of golf courses relative to Black neighborhood demographics and real-time weather data.

## Features

- **Spatial Search**: Discover golf courses within a specified radius of any zip code.
- **Demographic Insights**: Automatic enrichment of course data with Census tract demographics (Black, White, Hispanic, Asian, etc.).
- **Weather Integration**: Real-time temperature fetching for each course location using the WeatherNext BigQuery dataset.
- **Course Details**: Detailed view for each course including contact info, weather, and a demographic pie chart.
- **Dark Mode**: Fully implemented dark mode for low-light research.
- **Data Export**: Unique course data is automatically cached and exported to GCS in JSON format.

## Setup

1. **Environment Variables**: Use a `.env` file to store references to Google Cloud secrets.
    - `SECRET_PLACES`: Google Maps API Key
    - `SECRET_CENSUS_API`: Census Bureau API Key
    - `SECRET_BUCKET`: GCS Bucket Name for caching
    - `GOOGLE_CLOUD_PROJECT`: Your GCP Project ID
2. **Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```
3. **Run**:
    ```bash
    python app.py
    ```

## Technology Stack

- **Backend**: Python (Flask)
- **Frontend**: HTML5, Vanilla CSS, Tailwind CSS, Chart.js
- **APIs**: Google Maps, US Census Bureau
- **Data Warehousing**: BigQuery (WeatherNext)
- **Storage**: Google Cloud Storage