# Sora Union ETL

This repository contains the ETL pipeline for Sora Union, built with Python and Prefect for orchestration. The pipeline processes data from multiple sources, ensuring efficient extraction, transformation, and loading (ETL) into the target database.



## Dimensional data model
The ETL pipeline follows a dimensional data model, consisting of the following tables:
![ETL Architecture](images/sora-union-data-modelling.png)


## ETL architecture
The ETL pipeline consists of the following components:
This is the path: images/sora-union-data-modelling.png

![ETL Architecture](images/sora-unoin-etl-architecture.jpg)



## Setup Instructions

### 1. Clone the Repository

First, clone the repository to your local machine:

```bash
git clone https://github.com/dit-x/sora_union_etl.git
cd sora_union_etl
```

### 2. Set Up a Virtual Environment
``` bash
# Create a virtual environment (venv)
python3 -m venv venv

# Activate the virtual environment
# On macOS/Linux:
source venv/bin/activate

# On Windows:
venv\Scripts\activate
```

### 3. Install Required Dependencies
Once the virtual environment is active, install the required Python packages by running:

``` bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables
You need to set the configuration path for the project in the .env file. This file should contain the necessary environment variables required for running the ETL pipeline.

Create a `.env` file in the root directory of the project and set the `CONFIG_PATH` variable:

``` bash
# .env file
CONFIG_PATH=<path_to_your_config_file>
```


### 5. Start Prefect Server
To run the ETL pipeline with Prefect, you need to start the Prefect server. Open a terminal and run:

```bash
prefect server start
```
This will start the Prefect server, allowing you to manage flows, monitor tasks, and view logs.

### 6. Run the ETL Pipeline
Finally, to run the ETL pipeline, execute the main script:

```bash
python main.py
``` 
This will trigger the Prefect flow defined in main.py and begin the ETL process.

## Additional Information
- Monitoring: You can monitor your flows by accessing the Prefect dashboard at http://localhost:4200 (if using the default settings).

- Configuration: The project is designed to dynamically load configurations based on the path set in the `.env` file.