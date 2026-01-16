import pandas as pd
from prophet import Prophet
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

import pandas as pd
from prophet import Prophet
import os
import logging
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_forecast(df, target_col='total_updates', periods=30):
    """
    Generic forecasting function using Prophet.
    Args:
        df: DataFrame with 'date' and the target column.
        target_col: The column name to forecast.
        periods: Number of days to forecast.
    Returns:
        forecast_df: Predicted values with upper/lower bounds.
        model: Trained Prophet model object.
    """
    if df.empty:
        return pd.DataFrame(), None

    # Prepare data for Prophet (ds, y)
    prophet_df = df.groupby('date')[target_col].sum().reset_index()
    prophet_df.rename(columns={'date': 'ds', target_col: 'y'}, inplace=True)
    
    # Check if we have enough data points (Prophet likes at least 2)
    if len(prophet_df) < 2:
        return pd.DataFrame(), None

    try:
        # Train Prophet Model
        model = Prophet(
            yearly_seasonality=False, 
            weekly_seasonality=True, 
            daily_seasonality=False,
            changepoint_prior_scale=0.05
        )
        model.fit(prophet_df)
        
        # Predict
        future = model.make_future_dataframe(periods=periods)
        forecast = model.predict(future)
        
        return forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']], model
    except Exception as e:
        logging.error(f"Error in forecasting: {e}")
        return pd.DataFrame(), None

def generate_forecast_insights(forecast, model, historical_df, target_col):
    """
    Generates textual insights based on forecast components.
    """
    if forecast.empty or model is None:
        return "Not enough data to generate detailed insights."

    insights = []
    
    # 1. Trend Analysis
    df_tail = forecast.tail(30)
    start_val = df_tail['yhat'].iloc[0]
    end_val = df_tail['yhat'].iloc[-1]
    percent_change = ((end_val - start_val) / start_val) * 100 if start_val != 0 else 0
    
    trend_desc = "increasing" if percent_change > 2 else ("decreasing" if percent_change < -2 else "stable")
    insights.append(f"The overall trend for **{target_col.replace('_', ' ').title()}** is projected to be **{trend_desc}** over the next 30 days, with an estimated change of **{percent_change:.1f}%**.")

    # 2. Seasonality (Weekly)
    # Check if weekly seasonality was found
    if 'weekly' in model.seasonalities:
        # We can look at the periods to see which day is highest
        # This is a bit complex to extract directly from the model object's internals without more plotting
        # but we can look at the forecast 'weekly' component if it exists
        if 'weekly' in forecast.columns:
            # Group by day of week
            forecast['day_of_week'] = forecast['ds'].dt.day_name()
            weekly_avg = forecast.groupby('day_of_week')['weekly'].mean()
            peak_day = weekly_avg.idxmax()
            low_day = weekly_avg.idxmin()
            insights.append(f"Highest activity is typically observed on **{peak_day}s**, while **{low_day}s** usually see a dip in volume.")

    # 3. Peak Prediction
    peak_row = df_tail.loc[df_tail['yhat'].idxmax()]
    insights.append(f"The model predicts a peak in activity on **{peak_row['ds'].strftime('%Y-%m-%d')}** with approximately **{peak_row['yhat']:,.0f}** events.")

    # 4. Confidence
    avg_width = (df_tail['yhat_upper'] - df_tail['yhat_lower']).mean()
    if avg_width / end_val < 0.2:
        confidence = "High"
    elif avg_width / end_val < 0.5:
        confidence = "Moderate"
    else:
        confidence = "Low (Highly Volatile)"
    
    insights.append(f"**Confidence Level:** {confidence}. This is calculated based on historical variance and data consistency.")

    return insights

def perform_forecasting():
    """
    Main entry point for CLI usage (backward compatibility).
    """
    data_path = os.path.join(os.path.dirname(__file__), '../../data/india_aggregated.csv')
    
    if not os.path.exists(data_path):
        logging.error(f"Data file not found at {data_path}")
        return

    df = pd.read_csv(data_path)
    df['date'] = pd.to_datetime(df['date'])
    
    forecast, model = get_forecast(df)
    if not forecast.empty:
        insights = generate_forecast_insights(forecast, model, df, 'total_updates')
        print("\n--- Forecast Insights ---")
        for insight in insights:
            print(f"- {insight}")
        return forecast
    return None

if __name__ == "__main__":
    perform_forecasting()
