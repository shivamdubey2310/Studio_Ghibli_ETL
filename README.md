# Studio Ghibli ETL Pipeline

A comprehensive ETL (Extract, Transform, Load) pipeline that extracts data from the Studio Ghibli API, transforms it using pandas, and loads it into MongoDB for analysis and storage. The project includes both standalone Python ETL scripts and Apache Airflow DAGs for orchestrated data pipelines.

**Base URL:** https://ghibliapi.vercel.app/

## ✨ Features

- **Standalone ETL Script**: Direct Python execution for immediate data processing
- **Apache Airflow Integration**: Orchestrated workflows using Astro CLI
- **Data Validation**: Comprehensive logging and error handling
- **Multiple Data Sources**: Films, people, locations, species, and vehicles
- **MongoDB Storage**: Scalable NoSQL database integration

## 📋 Overview

This project demonstrates a complete ETL workflow by:
- Extracting movie, character, and location data from the Studio Ghibli API
- Cleaning and transforming the data using pandas
- Storing the processed data in MongoDB for future analysis

## 🔄 ETL Flow

1. **Extract**: Fetch data from Studio Ghibli API endpoints using `requests`
2. **Transform**: Clean, normalize, and structure data using `pandas`
3. **Load**: Insert transformed data into MongoDB collections using `pymongo`

## 📚 API Endpoints

The project extracts data from the following endpoints:
- `/films` - Studio Ghibli movies
- `/people` - Characters from the movies
- `/locations` - Locations featured in the films
- `/species` - Different species in the Ghibli universe
- `/vehicles` - Vehicles used in the movies

## 🚀 Usage

### Standalone Execution

Activate your virtual environment and run the ETL pipeline:
```bash
source new_env/bin/activate  # Activate virtual environment
python Studio_Ghibli_ETL.py
```

### Airflow/Astro Execution

1. Start the Astro environment:
   ```bash
   cd astroProject
   astro dev start
   ```

2. Access Airflow UI at `http://localhost:8080`
   - Username: `admin`
   - Password: `admin`

3. Trigger the Studio Ghibli ETL DAG from the Airflow UI

### Monitoring

Check the ETL execution logs:
```bash
# View the log file
tail -f ETL_log.log
```

## 📁 Project Structure

```
Studio_Ghibli_ETL/
├── Studio_Ghibli_ETL.py    # Main ETL script
├── ETL_log.log             # Execution logs
├── species_raw.json        # Raw species data cache
├── README.md               # Project documentation
├── new_env/                # Python virtual environment
├── astroProject/           # Apache Airflow project (Astro CLI)
│   ├── dags/               # Airflow DAGs
│   │   ├── Studio_Ghibli_ETL.py  # Airflow version of ETL
│   │   ├── exampledag.py   # Example DAG
├── raw_json/               # Raw API response cache
│   ├── films_raw.json
|   ├── ...  
│   └── vehicles_raw.json
└── sample_json/            # Sample data for testing
    ├── films_sample.json
    ├── ...  
    └── vehicles_sample.json

```

## 📊 Expected Output

After successful execution, your MongoDB will contain collections:
- `films` - Movie information and metadata
- `characters` - Character details and relationships
- `locations` - Location data with geographical information
- `species` - Species information and characteristics
- `vehicles` - Vehicle data and specifications

### Log Files
- `ETL_log.log` - Contains detailed execution logs, errors, and processing statistics
- Raw JSON files cached in `raw_json/` directory for offline processing

## 📝 Development Notes

- The project uses a virtual environment (`new_env/`) for dependency isolation
- Raw data is cached locally to reduce API calls during development
- Both standalone and Airflow execution methods are supported
- Comprehensive logging is implemented for debugging and monitoring

## 📄 License

This project is for educational purposes and uses the public Studio Ghibli API.