# MemeDD Dashboard - Streamlit Version

This is a Streamlit recreation of the original Taipy-based MemeDD Dashboard project.

## Features

- **Bot Dashboard**: Monitor arbitrage bot performance with interactive charts
- **Data Visualization**: Hourly profit aggregation with month filtering
- **Real-time Data**: Connected to PostgreSQL database for live trading data
- **Responsive Design**: Modern UI with sidebar navigation

## Installation

1. Install the required dependencies:
```bash
pip install -r requirements.txt
```

2. Set up configuration:
   - Copy `secrets.toml.example` to `.streamlit/secrets.toml`
   - Add your database credentials and OpenAI API key
   - Or set environment variables for sensitive data

## Google AI (Gemini) Integration Setup

To enable AI-powered coin analysis chat using the free Google Gemini API:

1. **Get Google AI API Key**:
   - Visit [Google AI Studio](https://makersuite.google.com/app/apikey)
   - Sign in with your Google account
   - Create a free API key

2. **Configure API Key**:
   - Add to `.streamlit/secrets.toml`:
     ```toml
     [GEMINI_CONFIG]
     api_key = "your-google-ai-api-key-here"
     ```
   - Or set environment variable:
     ```bash
     export GEMINI_API_KEY="your-api-key-here"
     ```

3. **Features Available**:
   - AI-powered coin analysis using Gemini 1.5 Flash (free tier)
   - Trade insights and recommendations
   - Market condition analysis
   - Arbitrage strategy explanations
   - Cost-effective with generous free tier limits

## Running the Application

1. Start the Streamlit app:
```bash
streamlit run streamlit_app.py
```

2. Open your browser and navigate to the URL shown in the terminal (usually `http://localhost:8501`)

## Pages

### Bot Dashboard
- **Arbitrage Bot**: View profit over time with interactive charts
- **Sniper Bot**: Placeholder for future sniper bot data
- **Failed Sniper Bot**: Placeholder for failed sniper bot data

### Arb Info
- Information page for arbitrage operations (content to be added)

### BTS Info
- Information page for BTS operations (content to be added)

## Database Configuration

The application connects to a PostgreSQL database with the following configuration:
- Host: 51.195.190.115
- Port: 30008
- Database: memedd50
- Table: arbtransaction

## Features

- **Interactive Charts**: Plotly-based scatter plots for profit visualization
- **Month Filtering**: Filter data by specific months or view all data
- **Real-time Updates**: Data is cached for 5 minutes and automatically refreshed
- **Summary Statistics**: Key metrics displayed in metric cards
- **Responsive Layout**: Wide layout optimized for dashboard viewing

## Data Processing

- Hourly aggregation of profit data
- Automatic date parsing and formatting
- Error handling for database connection issues
- Data validation and cleaning

## Differences from Original Taipy Version

- Uses Streamlit instead of Taipy GUI
- Simplified navigation with sidebar
- Enhanced data visualization with Plotly
- Improved error handling and user feedback
- More responsive and modern UI design
