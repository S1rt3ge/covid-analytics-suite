# COVID Analytics Suite - Multi-Source Data Platform

A comprehensive COVID-19 analytics dashboard that integrates multiple data sources including Johns Hopkins University, WHO, Robert Koch Institute, and vaccination data from Our World in Data. Built with FastAPI, Snowflake data warehouse, and MongoDB for real-time data visualization and predictive analytics.

## Features

- **🌍 Multi-Source Data Integration**: Johns Hopkins University, WHO, Robert Koch Institute, ECDC, Our World in Data
- **📊 Interactive Dashboard**: Real-time COVID-19 analytics with Plotly visualizations
- **🔮 Predictive Analytics**: ARIMA-based forecasting for infection trends
- **🏥 Comprehensive Metrics**: Deaths, confirmed cases, vaccination rates, travel restrictions
- **🗃️ Dual Database Architecture**: Snowflake for analytics, MongoDB for metadata
- **⚡ Real-time API**: FastAPI-powered REST endpoints
- **🎯 Cross-validation**: Data quality checks across multiple sources
- **📱 Responsive Design**: Mobile-friendly dashboard interface

## Prerequisites

Before you begin, ensure you have met the following requirements:

- **Python 3.7+** installed on your machine
- **pip** package manager
- **Git** (for cloning the repository)
- **Snowflake Account**: Active Snowflake account with COVID-19 datasets
- **MongoDB Atlas Account**: MongoDB cloud database or local MongoDB installation
- **Network access** to both Snowflake and MongoDB instances

### Required Snowflake Tables
Your Snowflake database should contain these tables:
- `OPTIMIZED_JHU_COVID_19_TIMESERIES` - Johns Hopkins timeseries data
- `RKI_GER_COVID19_DASHBOARD` - German COVID-19 data
- `WHO_SITUATION_REPORTS` - WHO reports
- `ECDC_GLOBAL` - European CDC data
- `OWID_VACCINATIONS` - Vaccination data
- `HUM_RESTRICTIONS_AIRLINE` - Travel restrictions
- `OPTIMIZED_RKI_GER_COVID19_DASHBOARD` - German COVID-19 data
- `OPTIMIZED_WHO_SITUATION_REPORTS` - WHO reports
- `OPTIMIZED_ECDC_GLOBAL` - European CDC data
- `OPTIMIZED_OWID_VACCINATIONS` - Vaccination data
- `OPTIMIZED_HUM_RESTRICTIONS_AIRLINE` - Travel restrictions

## Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/your-repo-name.git
   cd your-repo-name
   ```

2. **Create a virtual environment** (recommended)
   ```bash
   python -m venv venv
   
   # Activate the virtual environment
   # On Windows:
   venv\Scripts\activate
   
   # On macOS/Linux:
   source venv/bin/activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Environment Configuration**
   
   Create a `.env` file in the root directory and add your API keys and configuration variables:
   ```bash
   cp .env.example .env
   ```
   
   Edit the `.env` file and add your actual API keys and another info:
   ```env
   # Example environment variables - replace with your actual keys
   API_KEY=your_api_key_here
   SECRET_KEY=your_secret_key_here
   DATABASE_URL=your_database_url_here
   DEBUG=True
   ```

## Configuration

### Environment Variables

The following environment variables need to be set in your `.env` file:

| Variable | Description | Required | Example |
|----------|-------------|----------|---------|
| `SNOWFLAKE_ACCOUNT` | Your Snowflake account identifier | Yes | `xy12345.us-east-1` |
| `SNOWFLAKE_USER` | Snowflake username | Yes | `your_username` |
| `SNOWFLAKE_PASSWORD` | Snowflake password | Yes | `your_secure_password` |
| `SNOWFLAKE_WAREHOUSE` | Snowflake warehouse name | Yes | `COMPUTE_WH` |
| `SNOWFLAKE_DATABASE` | Snowflake database name | Yes | `ANALYTICS_DB` |
| `SNOWFLAKE_SCHEMA` | Snowflake schema name | Yes | `PUBLIC` |
| `SNOWFLAKE_ROLE` | Snowflake role name | Yes | `ACCOUNTADMIN` |
| `MONGODB_URI` | MongoDB connection string | Yes | `mongodb+srv://user:pass@cluster.mongodb.net/` |
| `MONGODB_DB` | MongoDB database name | Yes | `analytics_db` |
| `MONGODB_COUNTRY_COL` | MongoDB collection name | Yes | `country_data` |
| `API_BASE` | Local API server base URL | No | `http://127.0.0.1:8080` |

### Database Setup

#### Snowflake Setup
1. **Create a Snowflake Account**: Sign up at [Snowflake](https://signup.snowflake.com/)
2. **Note your account identifier**: Found in your Snowflake URL (e.g., `xy12345.us-east-1.snowflakecomputing.com`)
3. **Create necessary warehouses, databases, and schemas** as needed for your project
4. **Ensure your user has appropriate permissions** to access the specified warehouse, database, and schema

#### MongoDB Setup
1. **MongoDB Atlas (Recommended)**:
   - Sign up at [MongoDB Atlas](https://www.mongodb.com/cloud/atlas)
   - Create a new cluster
   - Create a database user with read/write permissions
   - Whitelist your IP address or use 0.0.0.0/0 for testing
   - Get your connection string from the "Connect" button

2. **Local MongoDB**:
   - Install MongoDB locally
   - Start the MongoDB service
   - Use connection string: `mongodb://localhost:27017/your_database_name`

## Usage

### Starting the Application

1. **Start the FastAPI server**
   ```bash
   python main.py
   ```
   
   Or use uvicorn directly:
   ```bash
   uvicorn main:app --host 0.0.0.0 --port 8000 --reload
   ```

2. **Access the application**
   - **Dashboard**: http://localhost:8000/dashboard/
   - **API Documentation**: http://localhost:8000/docs
   - **Health Check**: http://localhost:8000/health

### API Endpoints

#### Core Endpoints
- `GET /` - Application info and available endpoints
- `GET /health` - System health check with database status
- `GET /health?verbose=1` - Detailed health check with data sources

#### COVID Data Endpoints
- `GET /covid/*` - COVID-19 data endpoints (see `/docs` for full list)
- `GET /analytics/*` - Advanced analytics endpoints
- `GET /dashboard/*` - Dashboard-specific endpoints

#### Dashboard Features
- **Country Selection**: Choose from predefined country groups or individual countries
- **Time Range Analysis**: Select specific date ranges for analysis
- **Multiple Visualizations**: Daily trends, mortality analytics, infection rates
- **Predictive Modeling**: Click "Predict Infections" for forecasting
- **Team Annotations**: Add and view team insights directly in the dashboard

## Project Structure

```
covid-analytics-suite/
│
├── app/                    # Application modules
│   ├── routers/           # FastAPI routers
│   │   ├── analytics.py   # Analytics endpoints
│   │   ├── covid.py       # COVID data endpoints
│   │   └── dashboard.py   # Dashboard endpoints
│   ├── models/            # Pydantic models
│   │   └── schemas.py     # Data schemas
│   └── database/          # Database connections
│       └── mongodb.py     # MongoDB utilities
│
├── utils/                 # Utility functions and helpers
│   ├── __pycache__/      # Python cache files (ignored by git)
│   ├── cache.py          # Caching functionality
│   ├── __init__.py       # Package initialization
│   └── config.py         # Configuration settings
│
├── static/               # Static web assets
│   ├── assets/          # Images, fonts, etc.
│   ├── css/             # Stylesheets
│   │   └── styles.css   # Main stylesheet
│   └── js/              # JavaScript files
│       └── script.js    # Dashboard functionality
│
├── templates/           # HTML templates
│   └── dashboard.html   # Main dashboard template
│
├── tests/              # Test files
│
├── main.py            # Main FastAPI application entry point
├── requirements.txt   # Python dependencies
├── .env              # Environment variables (not in git)
├── .env.example     # Environment template
├── .gitignore       # Git ignore rules
└── README.md        # This file
```

## Development

### Setting up for Development

1. Follow the installation steps above
2. Install development dependencies (if you have any):
   ```bash
   pip install -r requirements-dev.txt  # if you have dev requirements
   ```

### Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Commit your changes (`git commit -m 'Add some amazing feature'`)
5. Push to the branch (`git push origin feature/amazing-feature`)
6. Open a Pull Request

## Testing

If you have tests, describe how to run them:

```bash
python -m pytest tests/
# or
python -m unittest discover tests/
```

## Deployment

Instructions for deploying your application:

### Local Deployment
```bash
python main.py
```

### Production Deployment
- Add specific deployment instructions for your hosting platform
- Include any necessary environment variable configurations
- Database setup instructions if applicable

## Troubleshooting

### Common Issues

1. **ImportError or ModuleNotFoundError**
   - Make sure your virtual environment is activated
   - Install all requirements: `pip install -r requirements.txt`

2. **API Connection Issues**
   - Check your `.env` file for correct API keys
   - Verify API key permissions and quotas

3. **Permission Errors**
   - Make sure you have the necessary permissions for file operations
   - Check if any files are being used by other processes

## License

This project is licensed under the [MIT License](LICENSE) - see the LICENSE file for details.

## Contact

My mail - buryanov.alexey@gmail.com

Project Link: https://github.com/S1rt3ge/covid-analytics-suite/tree/main

## Acknowledgments

- List any resources, libraries, or tutorials that helped you build this project
- Credit any APIs or services you're using
