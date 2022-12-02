# E-Commerce Data Analytics Pipeline

## Project Details

Project Period: 2022-04 ~ Present (Continuing Development)

Project Description: ''

Tag : `Python` `Scrapy` `Playwright` `EC2` `S3` `EMR` `GLUE` `Redshift` `Athena` `Tableau`

## Architecture
<img width="1000" alt="Screenshot 2022-11-29 at 11 16 09 AM" src="https://user-images.githubusercontent.com/24248797/205289493-e78b032b-55cc-4c97-a6c1-9f36bb54a6c4.png">


1. ECS (E-Comm. Scraper) collects product data of a specific brand from several domestic/foreign marketplaces.
2. When the collection is finished, the collection data is loaded with raw buckets.
3. Load the table of collection data and official product data from EMR, perform *'Asset Matching'. And, load it into the Processed Bucket.
4. Link from Redshift via Spectrum or export the data (asset matching completed data) from EMR using JDBC to Redshift's table.
5. Perform statistics and analysis on Redshift.
6. By linking Redshift with BI Tool (Tableau), the BI dashboard is constructed by visualizing statistics for each indicator.
7. Use the BI dashboard.

+ Redshfit can be used alternatively with Athena.
