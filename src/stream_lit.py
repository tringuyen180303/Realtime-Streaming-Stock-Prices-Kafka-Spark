import streamlit as st
import pandas as pd
import os
from datetime import datetime

# Set up the Streamlit app
st.title("Stock Prices Visualization")

# Define the path to the CSV files that are being saved by Spark Streaming
csv_path = "/tmp/kafka_to_csv_output"
today_date = datetime.today().strftime('%Y-%m-%d')
csv_path = os.path.join(csv_path, today_date)

# Function to load the latest CSV data
def load_latest_data():
    # Get the most recent CSV file from the directory
    files = [f for f in os.listdir(csv_path) if f.endswith('.csv')]
    if not files:
        st.error("No CSV files found in the specified directory.")
        return None
    
    latest_file = max(files, key=lambda x: os.path.getmtime(os.path.join(csv_path, x)))
    latest_file_path = os.path.join(csv_path, latest_file)
    
    # Load the CSV data into a pandas DataFrame
    df = pd.read_csv(latest_file_path)
    return df

# Load the data
df = load_latest_data()

if df is not None:
    # extract the datetime only
    time = df['Datetime'].iloc[-1]
    print(time)
    time = df['Datetime'].iloc[-1].split(' ')[0]
    # Extract stock symbol from the DataFrame
    stock_symbol = df['symbol'].iloc[0]
    st.subheader(f"Latest Stock Price Data {time} for {stock_symbol}")
    st.write(df)

    # Convert 'Datetime' column to datetime format
    df['Datetime'] = pd.to_datetime(df['Datetime'])

    # Allow users to select columns for the chart
    chart_columns = st.multiselect(
        'Select columns to visualize',
        options=['Open', 'High', 'Low', 'Close', 'Volume'],
        default=['Open']
    )

    # Plot the line chart for selected columns
    st.subheader(f"Stock Prices Over Time {time} for {stock_symbol}")
    st.line_chart(df[['Datetime'] + chart_columns].set_index('Datetime'))

# Run the Streamlit app
if __name__ == "__main__":
    st.write("Streamlit app for visualizing real-time stock prices.")
