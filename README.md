# Black Golf Research

A web application to explore and analyze the geographic distribution of golf courses relative to Black neighborhood demographics and real-time weather data.

## Features

- **Spatial Search**: Discover golf courses within a specified radius of any zip code.
- **Demographic Insights**: Automatic enrichment of course data with Census tract demographics (Black, White, Hispanic, Asian, etc.).
- **Weather Integration**: Real-time temperature fetching for each course location using the WeatherNext BigQuery dataset.
- **Course Details**: Detailed view for each course including contact info, weather, and a demographic pie chart.
- **Dark Mode**: Fully implemented dark mode for low-light research.
- **Data Export**: Unique course data is automatically cached and exported to GCS in JSON format.

## Key Improvements (Jan 2026)

- **Enhanced Visualizations**: Improved plotting in `analysis.py` with larger fonts and a new demographic split chart that includes **Plurality Black** neighborhoods.
- **Accessibility & Branding**: Refined the UI for better WCAG compliance. The "View Details" modal and current weather sections now feature theme-aware text colors.
- **Dynamic Styling**: The wind arrow visualization now toggles its color palette (Indigo/Gold) based on the active theme (Light/Dark mode).

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

## GCP Setup
1. use bash deploy.sh
2. If error, go to artifacts registry via GCP console and deploy manually
3. Run when deploying on GCP with public access prevention:
gcloud run services proxy black-golf-research-21jan26 --project pytutoring-dev --region us-east4

## Maintenance & Reliability

### Automated Task Agent
The project includes an automated agent to track progress and reliability:
```bash
python task_agent.py
```
This script:
1. Summarizes the last 5 commits from git.
2. Extracts the latest status from `comprehensive_test_report.md`.
3. Updates the `## Current Status` section in `task.md` with a timestamped report.

### Stress Testing
Use the test agent to verify API connectivity and response parsing:
```bash
python test_agent.py --limit 10
```

## Technology Stack
- **Backend**: Python (Flask)
- **Frontend**: HTML5, Vanilla CSS, Tailwind CSS, Chart.js
- **APIs**: Google Maps, US Census Bureau
- **Data Warehousing**: BigQuery (WeatherNext)
- **Storage**: Google Cloud Storage