# Development Roadmap: black-golf-research

## Current Status
- 

## Proposed Tasks
### **Backend Logic**
* [ ] Create a backend route for tracking all of the census tracts within a state. Include the same demographic information.

### **Social Equity Metrics (Priority)**
* [ ] **Neighborhood Economic Pressure: Poverty Rate**  
  * Logic: Update get\_demographics in maps\_golf\_lookup.py to fetch Census variable B17001.  
  * UI: Display the percentage of individuals below the poverty line in the View Details modal.  
* [ ] **Accessibility: Operational Status & Type**  
  * Logic: Extract and map Google Places types (e.g., country\_club) to a user-friendly classification: Municipal/Public, Semi-Private, or Private.  
  * UI: Add a "Barrier to Entry" status indicator to the modal.  
* [ ] **Historical Context: HOLC Redlining Grade**  
  * Logic: Implement a spatial lookup or API call (Mapping Inequality) to determine the 1930s HOLC grade for the course's tract.  
  * UI: Add a "Historical Zoning" badge (Grade A-D) to the modal.  


## Permanent Tasks
- Ensure libraries and code comply with spec.md and persona.md
- Update "Current Status" section of this document as needed
- Update README.md with a summary of the project and instructions on how to run it as needed.

## Future Tasks (DO NOT ATTEMPT YET)
* [ ] Add an agent that can automate the process of updating this task.md file with a summary of changes made and any issues it finds while testing the site. 
* [ ] Execute the refactor.md file.
### **Environmental Equity**
* [ ] **Environmental Equity: Cooling Differential**  
  * Logic: Compare WeatherNext 2m\_temperature from the course's coordinates against the average of the surrounding urban grid.  
  * UI: Display the "Cooling Effect" in degrees Fahrenheit in the modal weather section.  
* [ ] **Transit Connectivity Score**  
  * Logic: Use Google Maps Distance Matrix to compare walking vs. transit times from the nearest population center.  
  * UI: Add a "Transit Accessibility Index" (Low/Medium/High) to the modal.