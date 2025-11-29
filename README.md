# Claims Data Transformation Pipeline

This project implements a robust ETL (Extract, Transform, Load) pipeline for processing insurance claims data using PySpark. It is designed to be modular, configurable, and easy to monitor through detailed logging.

## Features
- **Scalable Processing**: Built on PySpark for handling large datasets.
- **Configurable**: Key settings (Spark master, app name, logging paths) are managed via `config/spark.conf`.
- **Robust Logging**: Comprehensive logging system with daily rotating log files and console output.
- **Modular Design**: Separation of concerns with distinct modules for ETL logic and utilities.

## Project Structure

```text
swissre/
├── config/
│   └── spark.conf       # Configuration file for Spark settings
├── data/
│   ├── claims_data.csv  # Input claims data
│   └── policyholder...  # Input policyholder data
├── logs/                # Directory for daily log files (auto-created)
├── src/
│   ├── etl_job.py       # Core ETL logic (Extract, Transform, Load functions)
│   └── utils.py         # Utility functions (Logging setup, Config loading)
├── main.py              # Entry point to run the application
├── requirements.txt     # Python dependencies
└── README.md            # Project documentation
```

## Configuration
The application is configured using `config/spark.conf`. You can modify this file to change:
- `spark.app.name`: The name of the Spark application.
- `spark.master`: The master URL (e.g., `local[*]`).
- `spark.eventLog.dir`: Directory for Spark event logs.

## Logging
The application uses a robust logging system to track execution and errors.
- **Location**: Logs are saved in the `logs/` directory at the project root.
- **File Naming**: Log files are named with the current date, e.g., `etl_job_2025-11-28.log`.
- **Rotation**: A new log file is created automatically for each day.
- **Console Output**: Logs are also printed to the console for real-time monitoring.

## How to Run

1.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

2.  **Run the Application**:
    ```bash
    python main.py
    ```

3.  **Check Output**:
    - Processed data will be saved to the `processed_claims_output` directory.
    - Check the console or `logs/` directory for execution details.

## Assumptions
- **Claim ID**: Prefixes `CL` denote Coinsurance, `RX` denote Reinsurance. Others are "Unknown".
- **Priority**: Claims > $4000 are marked as "Urgent".
- **Hash ID**: The system uses a local MD5 hash of the claim_id instead of the external MD4 API to ensure reliability and avoid network dependencies during processing.
