# Bitcoin Volume Spike Detector

This program, `aggr_max_vol_01.py`, analyzes Bitcoin (BTC) trading data from compressed CSV files (`.gz`) to detect significant trading volume spikes. It then stores these spikes in a local SQLite database for later analysis.

## Overview

The program reads compressed CSV files containing BTC trade data from a specified directory. It filters the data, aggregates it into 1-minute intervals, identifies volume spikes, and stores them in a database.

## Key Features

*   **Data Input:** Reads compressed CSV files (`.gz`) from a designated directory (`/home/astroill/BTC/aggr-server/data` by default).
*   **Data Filtering:** Filters data by date (`START_DATE`) and minimum volume (`MORE_BTC_THRESHOLD`).
*   **Data Aggregation:** Resamples data into 1-minute intervals and calculates summary statistics.
*   **Spike Detection:** Identifies volume spikes above a defined threshold (`MAXIMUM_VOLUME_THRESHOLD`).
*   **Database Storage:** Stores spike information in a local SQLite database (`btc.db`).
*   **Duplicate Prevention:** Avoids saving duplicate records with the same timestamp.
*   **Error Handling:** Includes robust error handling and logging to manage potential issues.
*   **Logging:** Includes robust logging to `aggr_max_vol.log` file.
*   **Output:** Prints the time, closing price, volume for each spike and the timestamp of the most recent record in the database.

## Prerequisites

*   Python 3.x
*   Required Python libraries:
    *   `pandas`
* `dbiLL.db_btc`

## Installation

1.  Clone or download the repository.
2.  Install the required Python packages:
    ```bash
    pip install pandas
    ```
    Also you need to add `dbiLL.db_btc`.

## Usage

1.  **Place Data:** Ensure that your compressed CSV (`.gz`) data files are in the directory specified by the `DATA_PATH` constant in the script. By default it is: `/home/astroill/BTC/aggr-server/data`. Files should have date in the name.
2.  **Run the Script:**
    ```bash
    python aggr_max_vol_01.py
    ```
3. **Logging**
    All errors and warnings will be logged to `aggr_max_vol.log`
4.  **Check for spikes.**
    The program will print spikes into console.
5. **Check DB**
    The detected spikes will be saved in `btc.db`.

## Configuration

The following constants in `aggr_max_vol_01.py` can be adjusted:

*   `MORE_BTC_THRESHOLD`: The minimum trading volume for a record to be considered. Default: `10`.
*   `MAXIMUM_VOLUME_THRESHOLD`: The minimum volume for a spike to be identified. Default: `500`.
*   `DATA_PATH`: The directory where the compressed CSV files are located. Default: `/home/astroill/BTC/aggr-server/data`.
*   `START_DATE`: The minimum date for the files that will be processed. Default: `2025-03-04`.

## Output

*   **Console Output:**
    *   Prints the exchange/pair, timestamp, closing price, and volume for each detected volume spike.
    *   Prints the timestamp of the most recent record saved in the database.
*   **Database:**
    *   Saves detected spikes in the `btc.db` SQLite database.
    * Stores `time`, `close`, `vol`, `dir`, `liq` for each record.
* **Logging:**
    * All errors and warnings will be saved in `aggr_max_vol.log`

## Data format
* File should be in `.gz` format.
* Each file's name is expected to include a date (YYYY-MM-DD) at the beginning of the name.
* Each `.gz` file contains data about BTC trades with the following space-separated columns: `time`, `close`, `vol`, `dir`, `liq`.

## License

[MIT License]
