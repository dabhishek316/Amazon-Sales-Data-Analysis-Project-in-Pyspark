# Amazon-Sales-Data-Analysis-Project-in-Pyspark
Amazon-Sales-Data-Analysis-Project-in-Pyspark

![image](https://github.com/user-attachments/assets/087b5d80-0bfc-46dc-bf96-b7f05e563992)


# Amazon Data Analysis Project in PySpark

This project demonstrates how to utilize PySpark for analyzing Amazon sales data to gain insights into sales trends, customer behavior, and product performance. The project includes data cleaning, querying, and feature engineering to highlight the capabilities of PySpark in the e-commerce domain.

## Project Overview

PySpark’s robust capability to handle large datasets makes it an essential tool for data processing and analysis across various industries. This project leverages PySpark to analyze Amazon sales data, uncovering insights into sales trends, customer behavior, and product performance.

### Key Objectives

1. **Identify the Region and Country with the Highest Profit**:
   - Group data by region to find the total profit for each region.
   - Identify the region with the highest profit.
   - Within that region, identify the country with the highest profit.

2. **Determine the Highest Number of Orders in a 72-Hour Period**:
   - Group data by a sliding window of 72 hours.
   - Count the number of orders within each 72-hour window.
   - Identify the period with the highest number of orders.

3. **Find the Month with the Highest Average Units Sold**:
   - Group data by month.
   - Calculate the average number of units sold for each month.
   - Identify the month with the highest average units sold.

4. **Calculate Lowest and Highest Margin Percentages**:
   - Compute the margin percentage for each order.
   - Identify the orders with the lowest and highest margin percentages.

5. **Identify Top 3 Highest-Value Orders for a Specific Item Type**:
   - Group data by item type and order by total profit.
   - Assign a row number to each order within its item type partition.
   - Filter to keep only the top 3 highest-value orders for each item type.

## Installation

1. **Install PySpark**:
   ```bash
   pip install pyspark

