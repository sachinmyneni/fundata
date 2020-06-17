# fundata
What can we find from data on the web?

First project is to scrape spotcrime.com, a site that aggregates crime reports from police departments that are more transparent. 

## Known Issues
- For missing data a row with too many columns is added. Each time the script is restarted we need to go remove all rows with 3 cosecutive commas till this issue is resolved.
- Every few hours there is a timeout.
