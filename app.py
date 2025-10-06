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

def detect_and_correct_abnormal_readings(meter_data):
    """
    Detect and correct abnormal readings where readings are multiples of normal pattern.
    Example: 143204.01, 286408.02 (2x), 143204.01
    """
    if len(meter_data) < 3:
        return meter_data
    
    # Sort by timestamp
    meter_data = meter_data.sort_values('Timestamp').reset_index(drop=True)
    
    # Calculate differences between consecutive readings
    meter_data['Reading_Diff'] = meter_data['Energy Reading'].diff()
    meter_data['Next_Reading_Diff'] = meter_data['Energy Reading'].diff(-1).abs()
    
    # Look for patterns where a reading is approximately multiple of adjacent readings
    for i in range(1, len(meter_data)-1):
        current_reading = meter_data.loc[i, 'Energy Reading']
        prev_reading = meter_data.loc[i-1, 'Energy Reading']
        next_reading = meter_data.loc[i+1, 'Energy Reading']
        
        # Check if current reading is approximately 2x of both neighbors
        if (abs(current_reading - 2 * prev_reading) / prev_reading < 0.01 and 
            abs(current_reading - 2 * next_reading) / next_reading < 0.01):
            # Replace with average of neighbors (more robust than simple division)
            corrected_reading = (prev_reading + next_reading) / 2
            meter_data.loc[i, 'Energy Reading'] = corrected_reading
            st.info(f"✅ Corrected abnormal reading at {meter_data.loc[i, 'Timestamp']}: {current_reading} → {corrected_reading}")
        
        # Check for 3x patterns
        elif (abs(current_reading - 3 * prev_reading) / prev_reading < 0.01 and 
              abs(current_reading - 3 * next_reading) / next_reading < 0.01):
            corrected_reading = (prev_reading + next_reading) / 2
            meter_data.loc[i, 'Energy Reading'] = corrected_reading
            st.info(f"✅ Corrected abnormal reading at {meter_data.loc[i, 'Timestamp']}: {current_reading} → {corrected_reading}")
    
    return meter_data

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
    
    # Detect and correct abnormal readings (multiples pattern)
    original_count = len(meter_data)
    meter_data = detect_and_correct_abnormal_readings(meter_data)
    corrected_count = len(meter_data)
    
    if original_count != corrected_count:
        st.warning(f"Applied corrections for abnormal readings in meter {meter_data['Meter'].iloc[0]}")
    
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
        
        # Convert timestamp back to original format
        result_df['Timestamp'] = result_df['Timestamp'].dt.strftime('%d/%m/%Y %H:%M')
        
        return result_df[['Timestamp', 'Meter', 'Volume Consumption']]
    else:
        # No valid readings for this meter
        result_df = pd.DataFrame({
            'Timestamp': master_timestamps,
            'Meter': meter_data['Meter'].iloc[0] if not meter_data.empty else 'Unknown',
            'Volume Consumption': 0.0
        })
        # Convert timestamp back to original format
        result_df['Timestamp'] = result_df['Timestamp'].dt.strftime('%d/%m/%Y %H:%M')
        return result_df

def read_excel_files(uploaded_files):
    """Read all uploaded Excel files and combine data"""
    all_data = []
    required_columns = ['Timestamp', 'Meter', 'Energy Reading']
    
    if not uploaded_files:
        st.error("No files uploaded.")
        return None
    
    st.write(f"Found {len(uploaded_files)} Excel files")
    
    for i, file in enumerate(uploaded_files):
        try:
            # Read Excel file
            df = pd.read_excel(file)
            
            # Validate required columns
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                st.warning(f"File {file.name} missing columns: {missing_columns}. Skipping.")
                continue
                
            all_data.append(df)
            st.success(f"✅ Successfully read {file.name}")
            
        except Exception as e:
            st.warning(f"Could not read file {file.name}: {str(e)}. Skipping.")
    
    if not all_data:
        st.error("No valid Excel data found in the uploaded files.")
        return None
    
    # Combine all data
    combined_data = pd.concat(all_data, ignore_index=True)
    return combined_data

def main():
    st.title("Meter Data Processing App")
    
    # Generate master timeline
    master_timestamps = generate_master_timeline()
    
    # File uploader for multiple Excel files
    uploaded_files = st.file_uploader(
        "Upload Excel files", 
        type=['xlsx', 'xls'], 
        accept_multiple_files=True,
        help="You can upload up to 8 Excel files"
    )
    
    if uploaded_files:
        # Show uploaded files
        st.write(f"**Uploaded files ({len(uploaded_files)}):**")
        for file in uploaded_files:
            st.write(f"- {file.name}")
        
        # Show configuration options
        with st.expander("⚙️ Advanced Settings"):
            col1, col2 = st.columns(2)
            with col1:
                tolerance_percentage = st.number_input(
                    "Tolerance Percentage",
                    min_value=0.1,
                    max_value=10.0,
                    value=1.0,
                    help="Percentage tolerance for detecting multiple patterns"
                )
            with col2:
                enable_correction = st.checkbox(
                    "Enable Automatic Correction",
                    value=True,
                    help="Automatically correct abnormal readings"
                )
        
        process_button = st.button("Process Data")
        
        if process_button:
            try:
                st.info("Processing data...")
                progress_bar = st.progress(0)
                
                # Read all Excel files
                combined_data = read_excel_files(uploaded_files)
                
                if combined_data is None:
                    return
                
                # Store original timestamp format for output
                original_timestamps = combined_data['Timestamp'].copy()
                
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
                        meter_data = combined_data[combined_data['Meter'] == meter].copy()
                        
                        result_df = calculate_meter_consumption(meter_data, master_timestamps)
                        
                        # Save to CSV in memory
                        csv_buffer = BytesIO()
                        # Convert to CSV string
                        csv_data = result_df.to_csv(index=False)
                        # Write CSV data to buffer
                        csv_buffer.write(csv_data.encode('utf-8'))
                        csv_buffer.seek(0)
                        
                        # Add CSV to ZIP
                        zip_file.writestr(f"{meter}.csv", csv_buffer.getvalue())
                        processed_meters += 1
                        progress_bar.progress(processed_meters / len(unique_meters))
                
                zip_buffer.seek(0)
                
                st.success(f"✅ Processing complete! Processed {processed_meters} meters.")
                
                # Download button
                col1, col2 = st.columns(2)
                with col1:
                    st.download_button(
                        label="📥 Download ZIP File with CSV Output",
                        data=zip_buffer,
                        file_name="meter_consumption_data.zip",
                        mime="application/zip",
                        use_container_width=True
                    )
                
                with col2:
                    # Also provide option to download a sample CSV
                    if processed_meters > 0:
                        sample_meter = unique_meters[0]
                        sample_data = combined_data[combined_data['Meter'] == sample_meter].copy()
                        sample_result = calculate_meter_consumption(sample_data, master_timestamps)
                        sample_csv = sample_result.to_csv(index=False)
                        
                        st.download_button(
                            label="📄 Download Sample CSV",
                            data=sample_csv,
                            file_name=f"sample_{sample_meter}.csv",
                            mime="text/csv",
                            use_container_width=True
                        )
                
                # Show sample of processed data
                with st.expander("👀 Preview Processed Data Format"):
                    if processed_meters > 0:
                        sample_meter = unique_meters[0]
                        sample_data = combined_data[combined_data['Meter'] == sample_meter].copy()
                        sample_result = calculate_meter_consumption(sample_data, master_timestamps)
                        
                        st.write(f"Sample data for meter: {sample_meter}")
                        st.write("Timestamp format in output:", sample_result['Timestamp'].iloc[0] if not sample_result.empty else "No data")
                        st.dataframe(sample_result.head(10) if not sample_result.empty else "No data available")
                
                # Show statistics
                with st.expander("📊 Processing Statistics"):
                    st.write(f"Total Excel files processed: {len(uploaded_files)}")
                    st.write(f"Total meters processed: {processed_meters}")
                    st.write(f"Total timestamps in master timeline: {len(master_timestamps)}")
                    st.write(f"Date range: {master_timestamps[0].strftime('%d/%m/%Y')} to {master_timestamps[-1].strftime('%d/%m/%Y')}")
                    
                    # File size info
                    total_size = sum(file.size for file in uploaded_files)
                    st.write(f"Total input file size: {total_size / 1024 / 1024:.2f} MB")
                
            except Exception as e:
                st.error(f"❌ Processing failed: {str(e)}")
                st.info("Please check your Excel files and try again.")
    
    else:
        st.info("📁 Please upload one or more Excel files to proceed.")
        
        # Instructions
        with st.expander("📋 How to prepare your data"):
            st.markdown("""
            **Required Excel format:**
            - Each Excel file must have these columns: `Timestamp`, `Meter`, `Energy Reading`
            - Timestamp format: `DD/MM/YYYY HH:MM` (e.g., `01/01/2025 00:00`)
            - Meter: Meter identifier (text or number)
            - Energy Reading: Numeric energy reading value
            
            **Output format:**
            - Each meter will have its own CSV file in the output ZIP
            - CSV files contain: `Timestamp`, `Meter`, `Volume Consumption`
            
            **You can upload up to 8 Excel files at once**
            """)

if __name__ == "__main__":
    main()
