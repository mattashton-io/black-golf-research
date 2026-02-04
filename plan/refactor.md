# **Visual Refactor: Du Bois Data Portraits Style**

This document outlines the tasks required to transition the Black Golf Research UI and data visualizations to a style inspired by the W.E.B. Du Bois 1900 Paris Exposition charts.

## **Data Visualization Refactor (analysis.py)**
### **Task 7: Stacked Income Correlation Bar**

* [ ] Logic: Show the correlation between Median Income brackets and course proximity.  
* [ ] Styling: Use a segmented horizontal bar. Add thin connecting lines between different segments to show demographic shifts.

### **Task 8: The Research Funnel Pyramid**

* [ ] Logic: Create a visualization for the search process: Zips Scanned \-\> Courses Found \-\> Majority Black Found.  
* [ ] Styling: Render a 4-layer pyramid using the Indigo, Red, Gold, and Teal colors.

## **Infrastructure & Assets**

### **Task 9: Update Static Plot Handling**

* [ ] Ensure generate\_plots in analysis.py passes the facecolor='\#E8D9C5' argument to plt.savefig() to maintain the parchment background on exported PNGs.  
* [ ] Update app.py to ensure the new plot filenames are correctly mapped to the UI carousel.

### **Task 10: Map Style Sync**

* [ ] Update the Google Maps lightStyle in index.html to use a land color of \#E8D9C5 and water color of \#2C3E75 to match the choropleth map aesthetic.