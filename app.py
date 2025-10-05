import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import zipfile
from io import BytesIO
import numpy as np

def generate_master_timeline():
    start = datetime(2025, 1, 1, 0, 0)
    end = datetime(2025, 8, 31, 23, 45)
    timestamps = []
    current = start
    while current <= end:
        timestamps.append(current)
        current += timedelta(minutes=15)
    return timestamps

def calculate_meter_consumption(meter_data, master_timestamps):
    """Calculate consumption for a single meter with proper edge case handling"""
    if meter_data.empty:
        return pd.DataFrame({
            'Timestamp': master_timestamps,
            'Meter': meter_data.name if hasattr(meter_data, 'name') else 'Unknown',
            'Volume Consumption': 0.0
        })
    
    # Convert to numeric and clean
    meter_data = meter_data.copy()
    meter_data['Energy Reading'] = pd.to_numeric(meter_data['Energy Reading'], errors='coerce')
    
    # Remove rows where timestamp conversion failed
    meter_data = meter_data.dropna(subset=['Timestamp'])
    
    # Sort by timestamp
    meter_data = meter_data.sort_values('Timestamp')
    
    # Remove duplicates (keep first occurrence)
    meter_data = meter_data.drop_duplicates(subset=['Timestamp'], keep='first')
    
    # Get valid readings (non-null, non-negative counter values)
    valid_mask = meter_data['Energy Reading'].notna()
    
    # Special handling: if first valid reading is 0, it's acceptable
    if valid_mask.any():
        first_valid_idx = valid_mask.idxmax()
        valid_readings = meter_data[valid_mask].copy()
        
        # Calculate consumption
        valid_readings['Volume Consumption'] = valid_readings['Energy Reading'].diff()
        
        # First valid reading gets consumption = 0
        if not valid_readings.empty:
            valid_readings.iloc[0, valid_readings.columns.get_loc('Volume Consumption')] = 0
        
        # Replace negative consumption with 0
        valid_readings.loc[valid_readings['Volume Consumption'] < 0, 'Volume Consumption'] = 0
        
        # Merge with master timeline
        master_df = pd.DataFrame({'Timestamp': master_timestamps})
        result_df = master_df.merge(
            valid_readings[['Timestamp', 'Volume Consumption']], 
            on='Timestamp', 
            how='left'
        )
        result_df['Volume Consumption'] = result_df['Volume Consumption'].fillna(0.0)
        result_df['Meter'] = meter_data['Meter'].iloc[0] if not meter_data.empty else 'Unknown'
        
        return result_df[['Timestamp', 'Meter', 'Volume Consumption']]
    else:
        # No valid readings for this meter
        return pd.DataFrame({
            'Timestamp': master_timestamps,
            'Meter': meter_data['Meter'].iloc[0] if not meter_data.empty else 'Unknown',
            'Volume Consumption': 0.0
        })

def main():
    st.title("Meter Data Processing App")
    
    # Generate master timeline
    master_timestamps = generate_master_timeline()
    
    uploaded_files = st.file_uploader("Upload Excel files", type=['xlsx'], accept_multiple_files=True)
    
    if uploaded_files:
        process_button = st.button("Process Data")
        
        if process_button:
            try:
                st.info("Processing data...")
                progress_bar = st.progress(0)
                
                # Read and combine all files with validation
                all_data = []
                required_columns = ['Timestamp', 'Meter', 'Energy Reading']
                
                for i, file in enumerate(uploaded_files):
                    try:
                        df = pd.read_excel(file)
                        
                        # Validate required columns
                        missing_columns = [col for col in required_columns if col not in df.columns]
                        if missing_columns:
                            st.warning(f"File {file.name} missing columns: {missing_columns}. Skipping.")
                            continue
                            
                        all_data.append(df)
                    except Exception as e:
                        st.warning(f"Could not read file {file.name}: {str(e)}. Skipping.")
                
                if not all_data:
                    st.error("No valid data found in uploaded files.")
                    return
                
                combined_data = pd.concat(all_data, ignore_index=True)
                
                # Convert timestamp with error handling
                combined_data['Timestamp'] = pd.to_datetime(
                    combined_data['Timestamp'], 
                    format='%d/%m/%Y %H:%M', 
                    errors='coerce'
                )
                
                # Check for timestamp conversion failures
                failed_conversions = combined_data['Timestamp'].isna().sum()
                if failed_conversions > 0:
                    st.warning(f"Failed to convert {failed_conversions} timestamps. These rows will be ignored.")
                
                # Get unique meters
                unique_meters = combined_data['Meter'].unique()
                st.write(f"Found {len(unique_meters)} unique meters")
                
                # Process each meter
                zip_buffer = BytesIO()
                processed_meters = 0
                
                with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
                    for meter in unique_meters:
                        meter_data = combined_data[combined_data['Meter'] == meter]
                        result_df = calculate_meter_consumption(meter_data, master_timestamps)
                        
                        # Save to Excel in memory
                        excel_buffer = BytesIO()
                        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                            result_df.to_excel(writer, index=False, sheet_name='Consumption')
                        excel_buffer.seek(0)
                        
                        # Add to ZIP
                        zip_file.writestr(f"{meter}.xlsx", excel_buffer.getvalue())
                        processed_meters += 1
                        progress_bar.progress(processed_meters / len(unique_meters))
                
                zip_buffer.seek(0)
                
                st.success(f"✅ Processing complete! Processed {processed_meters} meters.")
                
                st.download_button(
                    label="📥 Download ZIP File",
                    data=zip_buffer,
                    file_name="meter_consumption_data.zip",
                    mime="application/zip"
                )
                
            except Exception as e:
                st.error(f"❌ Processing failed: {str(e)}")
                st.info("Please check your files and try again.")
    
    else:
        st.info("📁 Please upload one or more Excel files to proceed.")

if __name__ == "__main__":
    main()