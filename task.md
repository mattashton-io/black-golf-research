TO-DOs:
- [] For API call and app-loading efficiency, use GCS bucket data if a zipcode is re-entered after the first time. 
- [] Tweak the color scheme to be closer to Teal (#099268) instead of Red. I use https://yeun.github.io/open-color/#teal for reference.
- [] Add Light and Dark Mode toggle option
- [] Radius default to 10 miles by default, but allow user to change radius up to 25 mi
- [] If no golf course is found in a Black-majority area within default 10 mi, increase radius search until a course in a Black-majority area is found 
- [] Add user setting options to choose between filtering results by:
    - [] Strict Majority (>50%): The standard demographic definition.
    - [] Plurality: The highest racial percentage in the tract, even if under 50% (important for highly diverse urban areas).
    - [] Historical Significance: Joining with HOLC "Grade D" (Redlined) maps. This identifies courses that act as "buffers" between historically Black and White neighborhoods, even if the modern demographic has shifted.