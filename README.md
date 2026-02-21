# nsw-park-ride-tracker

**Background:** 
The Transport for NSW website provides real-time data on the number of available spots at Park&Ride car parks at any given point in time. However, it's still unclear when these car parks typically fill up.

**Solution:** 
Build an automated pipeline to analyse historical occupancy data and identify typical capacity trends.

**High-level pipeline:**
- Collect real-time occupancy data via the TfNSW Car park API throughout the day (every 10 min between 5am-10pm) and store in CSV files.
- Once per week, analyse the historical data to calculate insights for each car park (e.g., average available spots at different times of the day), which are stored in a JSON file.
- Insights data is then used by a [tool](https://nathanwangly.github.io/projects/parknride-tracker/) to generate visualisations. The code for this final step is available in a different repository (see [.js](https://github.com/nathanwangly/nathanwangly.github.io/blob/main/assets/js/carpark_tracker.js) and [.md](https://github.com/nathanwangly/nathanwangly.github.io/blob/main/_projects/carpark_tracker.md) files).

Data collection for this project began mid February 2026.
