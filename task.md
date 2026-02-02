# Development Roadmap: black-golf-research

## Current Status
* Fixing broken images when retrieving cached results from GCS.

## Proposed Tasks
* [ ] **Implement Unique Plot Filenames**  
  * Logic: Update analysis.py's generate\_plots function to accept zip\_code as an argument.  
  * Styling: Prefix all generated plot filenames with the zip\_code (e.g., 10001\_demographic\_distribution.png) to prevent collisions and support multi-user caching.  
* [ ] **Synchronize Plots to GCS**  
  * Logic: In app.py, after generate\_plots is called in the /search route, upload the resulting .png files from the local static/plots/ directory to the GCS bucket defined by SECRET\_BUCKET.  
  * Path: Store them under plots/{zip\_code}/ in the bucket.  
* [ ] **Resilient Plot Serving Route**  
  * Logic: Refactor the @app.route('/search\_plot/\<path:filename\>') in app.py.  
  * Behavior: Check if the file exists in the local static/plots/ directory. If missing (common in stateless Cloud Run environments), attempt to download the file from GCS using the SECRET\_BUCKET and the corresponding zip\_code prefix before serving.  
* [ ] **Update Cache JSON Structure**  
  * Logic: Ensure the result dictionary saved to save\_to\_zip\_cache contains the full relative path or unique filename for the plots, ensuring the frontend can request the specific unique images.


## Permanent Tasks
- Ensure libraries and code comply with spec.md and persona.md
- Update "Current Status" section of this document as needed
- Update README.md with a summary of the project and instructions on how to run it as needed.

## Future Tasks (DO NOT ATTEMPT YET)
- Add an agent that can automate the process of updating this task.md file with a summary of changes made and any issues it finds while testing the site. 