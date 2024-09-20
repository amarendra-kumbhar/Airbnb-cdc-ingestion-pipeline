# AirBnB CDC Ingestion Pipeline

## Overview
This project implements an end-to-end **Change Data Capture (CDC)** pipeline to process **customer** and **booking** data for AirBnB using **Azure Data Factory (ADF)**, **Azure Synapse**, **CosmosDB**, and **Python**. The pipeline ingests data incrementally, processes it in real time, and performs transformations and upserts in the target Synapse tables.

The pipeline uses **SCD Type 1** for customer data and **change feeds from CosmosDB** for booking events. Additionally, the system aggregates booking data on a per-customer basis in Azure Synapse.

## Tech Stack
- **Azure Data Factory (ADF)**: Ingests data from ADLS and CosmosDB and orchestrates data flows.
- **Azure Synapse**: Stores and processes customer and booking data.
- **CosmosDB**: Stores real-time booking events and acts as the source for CDC feeds.
- **Python**: Generates mock booking data and pushes it into CosmosDB.
- **SQL**: Defines table structures and a stored procedure for data aggregation.


## Project Structure
- **`airbnbdatapipeline/`**: ARM templates and parameters for pipeline deployment.
- **`dataflow/`**: Data transformation logic for Azure Data Factory.
- **`dataset/`**: Datasets used in Data Factory.
- **`factory/`**: Configuration files for Azure Data Factory.
- **`linkedService/`**: Linked Services definitions in ADF.
- **`pipeline/`**: ADF Pipelines JSON files.
- **`synapse/`**: SQL scripts for Azure Synapse (create tables, stored procedures).
- **`Sample customer dataset/`**: Sample customer data files for testing.
- **`python/`**: Python scripts for generating and pushing booking data to CosmosDB.


## Key Features
1. **Customer Data Ingestion:**
   - Ingests customer data from **Azure Data Lake Storage (ADLS)** on an **hourly basis** and loads it into the **customer_dim** table in Azure Synapse.
   - Uses **SCD Type 1** for customer data updates.

2. **Booking Data Ingestion (CDC):**
   - Reads **change feed** from **CosmosDB** to capture **real-time booking events**.
   - Transforms booking data and **upserts** it into the **booking_fact** table in Azure Synapse.

3. **Data Aggregation:**
   - A stored procedure aggregates booking data to calculate **total bookings**, **total booking amount**, and the **last booking date** for each customer in the **booking_aggregation** table.

## Azure Data Factory Pipelines

1. **Customer Data Pipeline**:
   - Ingests customer data from **ADLS** into the **customer_dim** table.
   - Moves raw data to the **archive folder** and deletes it from the source folder.
   
2. **Booking Data Pipeline**:
   - Ingests booking data from **CosmosDB** change feed into the **booking_fact** table.
   - Executes a **stored procedure** to perform aggregation on the booking data.

3. **Pipeline Orchestration**:
   - Orchestrates the execution of customer and booking pipelines in the correct order using **ADF's Execute Pipeline** activity.

### Step 1: Clone the Repository
Use one of the following commands to clone the repository:

**HTTPS:**
```bash
git clone https://github.com/your-username/airbnbdatapipeline.git
cd airbnbdatapipeline

Step 2: Set Up Azure Synapse
Create the customer_dim, booking_fact, and booking_aggregation tables by running the SQL scripts in the /synapse folder.
Execute the stored procedure to handle data aggregation.

Step 3: Set Up ADF Pipelines
Import the ADF pipelines from the /pipeline folder into Azure Data Factory.
Configure the linked services to connect to ADLS, CosmosDB, and Azure Synapse.

Step 4: Run the Python Mock Data Generator
Navigate to the /python folder.
Install the required packages.

Step 5: Trigger the Pipelines
In ADF, run the customer and booking pipelines to load the data into Synapse.
Use the ADF orchestration pipeline to manage dependencies and trigger the pipelines.

