# Adding Data from New Ad Platforms to MCDM

This guide will help you integrate new ad platforms into the MCDM structure to ensure seamless data flow and reporting. Follow these steps:

## 1. Import Data into BigQuery
Create a new table for the ad platform’s data in BigQuery.

- **Step**: Use the CSV export to upload data to BigQuery.
- **Naming Convention**: Follow the format `src_ads_<platform_name>_all_data` to maintain consistency.
- **Data Requirements**: Ensure your data has the following columns: `ad_id`, `spend`, `channel`, `date`; and at least some of the following columns: `likes`, `comments`, `views`, `conversion`, `engagements`, `impressions`, `purchase`, and any other relevant ones.

## 2. Update the DBT Models
Modify the DBT model to include the new platform’s data. You will need to update the `ads_basic_performance` model.

- **Step 1**: Open the file `models/ads_basic_performance.sql`.
- **Step 2**: Add a new `WITH` clause for the new ad platform:
    ```sql
    new_platform_data AS (
        SELECT *
        FROM `swift-hangar-436709-i3`.`dbt_agromovich`.`src_ads_<new_platform>_all_data`
    ),
    ```
    > **Note:** Replace `<new_platform>` with the actual name of the new ad platform. Ensure the column types in this new table are the same as those in the existing report.

- **Step 3**: Add a `SELECT` statement for the new platform:
    ```sql
    SELECT
        CAST(ad_id AS STRING) AS ad_id,
        clicks,
        impressions,
        spend,
        conversions,
        -- Add any other relevant fields --
    FROM new_platform_data
    UNION ALL
    ```

## 3. Test the Integration
Ensure the data is pulling through correctly:

- **Step**: Run the DBT model to refresh the view.
- **Command**: `dbt run --select ads_basic_performance`
- Check for any errors or data discrepancies.

## 4. Update Looker Studio Report
Once the data source has been updated in BigQuery, add it to the Looker Studio report:

- **Step 1**: Open Looker Studio and go to your report.
- **Step 2**: Add the new BigQuery table as a data source.
- **Step 3**: Update the visuals to include metrics from the new platform.

## 5. Some Tips on Metrics' Calculations

- I've used the following formulas to calculate:
  - **Engagements** for Facebook ads: `(clicks + likes + comments + shares + views + add_to_cart) AS engagements;`
  - **Post Click Conversions** for Facebook ads: 
    ```sql
    ROUND(CASE WHEN clicks > 0 THEN purchase / clicks ELSE 0 END, 2);
    ```
  - **Post View Conversions** for Facebook ads: 
    ```sql
    ROUND(CASE WHEN views > 0 THEN purchase / views ELSE 0 END, 2);
    ```
  - To calculate **Total Conversions** for TikTok, I summed `conversions` and `skan_conversion`.


---
