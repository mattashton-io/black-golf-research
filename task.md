# Development Roadmap: black-golf-research

## Current Status
* [Finalized] Unique plot filenames and GCS synchronization implemented. Images now serve reliably from GCS cache in stateless environments.

## Proposed Tasks
* [ ] In ModalDetails window, a bold font for the labels (e.g. "Feels Like:") that is heavier than the font for the values (e.g. "72°F").
* [ ] Create a DARK MODE version of the plots in Demographic Insights, make sure the background color in the plot itself changes to match the dark mode theme. For LIGHT MODE, continue to use the current colors. If a dark mode version does not exist, read in the cache and create a new plot based on the cached results. Save the newly created Dark Mode plot in the cache.

## Permanent Tasks
- Ensure libraries and code comply with spec.md and persona.md
- Update "Current Status" section of this document as needed
- Update README.md with a summary of the project and instructions on how to run it as needed.

## Future Tasks (DO NOT ATTEMPT YET)
- Add an agent that can automate the process of updating this task.md file with a summary of changes made and any issues it finds while testing the site. 