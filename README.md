# CSV to PostgreSQL Data Pipeline

This project provides a simple Python script to read data from a CSV file and load it into a PostgreSQL database.

## Prerequisites

- Python 3.x
- PostgreSQL Server

## Setup

1.  **Clone the repository (or download the files):**
    ```bash
    git clone <repository_url>
    cd <repository_directory>
    ```

2.  **Create a virtual environment:**
    ```bash
    python -m venv myenv
    ```

3.  **Activate the virtual environment:**
    -   On Windows:
        ```bash
        myenv\Scripts\activate
        ```
    -   On macOS/Linux:
        ```bash
        source myenv/bin/activate
        ```

4.  **Install the required dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

5.  **Update the database configuration:**
    Open `src/pipeline.py` and modify the following variables with your PostgreSQL credentials if they are different from the defaults:
    ```python
    DB_NAME = "postgres"
    DB_USER = "postgres"
    DB_PASS = "techlab"
    DB_HOST = "localhost"
    DB_PORT = "5432"
    ```

6.  **Prepare your data:**
    Place your CSV file in the `data` directory. The default file is `sample_data.csv`. If you use a different file, update the `CSV_FILE_PATH` variable in `src/pipeline.py`.

## Running the Pipeline

To execute the data pipeline, run the following command from the root directory of the project:

```bash
python src/pipeline.py
```

The script will:
1.  Connect to the PostgreSQL database.
2.  Drop the `sample_data` table if it already exists.
3.  Create a new `sample_data` table based on the CSV columns.
4.  Insert all the data from the CSV file into the table.
5.  Print status messages to the console.
