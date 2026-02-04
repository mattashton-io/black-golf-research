# Development Roadmap: black-golf-research

## Current Status
* **Last Updated:** 2026-02-04 15:24:34
  * **Recent Changes:**
    - new cache rules in spec.md (a5757f0)
    - first mac commit + more socialeconomic data (8109db9)
    - more build (2c6e02f)
    - adjusted dark mode (b556b88)
    - cloud build tbsht (a9020d4)
  * **Test Status:**
    - Total Zips: 1
    - Search Success: 1/1
    - Weather OK: 1/1
    - Enrichment OK: 1/1

## Proposed Tasks
* [x] Add an agent that can automate the process of updating this task.md file with a summary of changes made and any issues it finds while testing the site. Place under "Current Status" section. Include date of last update.

## Permanent Tasks
- Ensure libraries and code comply with spec.md and persona.md
- Update "Current Status" section of this document as needed
- Update README.md with a summary of the project and instructions on how to run it as needed.

## Future Tasks (DO NOT ATTEMPT YET)
* [ ] Execute the refactor.md file.
### **Environmental Equity**
* [ ] **Environmental Equity: Cooling Differential**  
  * Logic: Compare WeatherNext 2m\_temperature from the course's coordinates against the average of the surrounding urban grid.  
  * UI: Display the "Cooling Effect" in degrees Fahrenheit in the modal weather section.  
* [ ] **Transit Connectivity Score**  
  * Logic: Use Google Maps Distance Matrix to compare walking vs. transit times from the nearest population center.  
  * UI: Add a "Transit Accessibility Index" (Low/Medium/High) to the modal.