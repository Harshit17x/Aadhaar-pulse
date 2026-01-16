import streamlit as st
import pandas as pd
import pydeck as pdk
import plotly.express as px
import os
import sys
from datetime import timedelta
import random
import math
import plotly.graph_objects as go
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add src to python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from generation.mock_data import generate_mock_data
from models.forecast import get_forecast, generate_forecast_insights
from utils.ollama_client import OllamaClient

st.set_page_config(page_title="Aadhaar Pulse", layout="wide")

# Theme setup (can be customized further via .streamlit/config.toml)
st.markdown("""
<style>
    /* Premium Look */
    .main {
        background-color: #0e1117;
    }
    .stMetric {
        background: rgba(255, 255, 255, 0.05);
        padding: 15px;
        border-radius: 10px;
        border: 1px solid rgba(255, 255, 255, 0.1);
        transition: transform 0.2s ease;
    }
    .stMetric:hover {
        transform: translateY(-5px);
        background: rgba(255, 255, 255, 0.08);
    }
    /* Tab Styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 10px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 40px;
        white-space: pre-wrap;
        background-color: rgba(255, 255, 255, 0.05);
        border-radius: 5px 5px 0px 0px;
        padding: 5px 20px;
    }
    .stTabs [aria-selected="true"] {
        background-color: #ff4b4b !important;
        color: white !important;
    }
    /* Map Title */
    .map-header {
        font-size: 1.5rem;
        font-weight: 700;
        margin-top: 2rem;
        margin-bottom: 1rem;
        color: #ff4b4b;
    }
    /* Legend Styling */
    .legend-card {
        background: rgba(0,0,0,0.4);
        padding: 10px;
        border-radius: 5px;
        border: 1px solid rgba(255,255,255,0.1);
        font-size: 0.8rem;
    }
</style>
""", unsafe_allow_html=True)

def normalize_state_name(state):
    """Normalize inconsistent state names into a standard set."""
    if not isinstance(state, str) or state.strip() == "":
        return "Other"
    
    s = " ".join(state.split()).title()
    
    # Comprehensive mapping for Indian States/UTs
    mapping = {
        # UTs
        "Andaman & Nicobar Islands": "Andaman and Nicobar Islands",
        "Andaman And Nicobar Islands": "Andaman and Nicobar Islands",
        "Dadra & Nagar Haveli": "Dadra and Nagar Haveli and Daman and Diu",
        "Dadra And Nagar Haveli": "Dadra and Nagar Haveli and Daman and Diu",
        "Daman & Diu": "Dadra and Nagar Haveli and Daman and Diu",
        "Daman And Diu": "Dadra and Nagar Haveli and Daman and Diu",
        "The Dadra And Nagar Haveli And Daman And Diu": "Dadra and Nagar Haveli and Daman and Diu",
        "Dadra And Nagar Haveli And Daman And Diu": "Dadra and Nagar Haveli and Daman and Diu",
        # Jammu & Kashmir
        "Jammu & Kashmir": "Jammu and Kashmir",
        "Jammu And Kashmir": "Jammu and Kashmir",
        # Tamil Nadu
        "Tamilnadu": "Tamil Nadu",
        # Odisha
        "Odisha": "Odisha",
        "Odisa": "Odisha",
        "Orissa": "Odisha",
        # Chhattisgarh
        "Chhatisgarh": "Chhattisgarh",
        # West Bengal
        "West  Bengal": "West Bengal",
        "West Bangal": "West Bengal",
        "West Bengli": "West Bengal",
        "Westbengal": "West Bengal",
        "West Bengal": "West Bengal",
        # Uttarakhand
        "Uttaranchal": "Uttarakhand",
        # Pondicherry
        "Pondicherry": "Puducherry"
    }
    
    normalized = mapping.get(s, s)
    
    # Invalid constants/city names found in data
    invalid = ['100000', 'Balanagar', 'Idpl Colony', 'Darbhanga', 'Jaipur', 'Nagpur', 'Puttenahalli', 'Madanapalle', 'Raja Annamalai Puram']
    if normalized in invalid or normalized.isdigit():
        return "Other"
        
    return normalized

def load_data():
    data_path = os.path.join(os.path.dirname(__file__), 'data/district_flows.csv')
    if not os.path.exists(data_path):
        return pd.DataFrame()
    
    df = pd.read_csv(data_path)
    df['date'] = pd.to_datetime(df['date'])
    
    # Sanitization
    df['source_state'] = df['source_state'].apply(normalize_state_name)
    df['dest_state'] = df['dest_state'].apply(normalize_state_name)
    
    # Filter out 'Other'
    df = df[df['source_state'] != "Other"]
    df = df[df['dest_state'] != "Other"]
    
    return df

@st.cache_data(ttl=3600)
def load_india_data():
    data_path = os.path.join(os.path.dirname(__file__), 'data/india_aggregated.csv')
    if not os.path.exists(data_path):
        return pd.DataFrame()
    df = pd.read_csv(data_path)
    df['date'] = pd.to_datetime(df['date'])
    
    # Sanitization
    df['state'] = df['state'].apply(normalize_state_name)
    df = df[df['state'] != "Other"]
    
    return df

@st.cache_data(ttl=3600)  # Cache for 1 hour
def get_system_location():
    """Fetch system's real geographical position based on IP."""
    try:
        response = requests.get("http://ip-api.com/json/", timeout=5)
        if response.status_code == 200:
            data = response.json()
            if data.get("status") == "success":
                return data.get("lat"), data.get("lon")
    except Exception as e:
        st.sidebar.error(f"Error fetching location: {e}")
    
    # Fallback to center of India if geolocation fails
    return 20.5937, 78.9629

def main():
    st.markdown('<div style="display: flex; align-items: center; justify-content: space-between;">'
                '<h1 style="margin: 0;">🇮🇳 Aadhaar Pulse: Unified View</h1>'
                '<span style="background: #00FF00; color: #000; padding: 5px 15px; border-radius: 20px; font-weight: bold; font-size: 0.8rem;">● SYSTEM ONLINE</span>'
                '</div>', unsafe_allow_html=True)
    st.markdown("### Real-time Insights into Aadhaar Enrollment, Updates & Demographic Trends")

    # Load unified India data
    df_pulse = load_india_data()
    df_migration = load_data()
    
    if df_pulse.empty:
        st.error("India data not found! Please run `python3 src/processing/india_data_processor.py` first.")
        return
    
    df_display_col = 'total_updates'

    # Sidebar Controls
    st.sidebar.title("🛠️ Control Panel")
    
    with st.sidebar.expander("📅 Time & Region Filters", expanded=True):
        # Unified Date Range
        all_dates = pd.concat([df_pulse['date'], df_migration['date']])
        min_date = all_dates.min()
        max_date = all_dates.max()
        
        date_range = st.sidebar.date_input(
            "Select Date Range",
            [min_date, max_date],
            min_value=min_date,
            max_value=max_date
        )
        if isinstance(date_range, (list, tuple)):
            start_date = date_range[0]
            end_date = date_range[1] if len(date_range) > 1 else date_range[0]
        else:
            start_date = end_date = date_range
        
        # Unified State List
        all_states_pulse = set(df_pulse['state'].unique())
        all_states_mig = set(df_migration['source_state'].unique()) | set(df_migration['dest_state'].unique())
        all_states = sorted(list(all_states_pulse | all_states_mig))

        # Default to showing a manageable number of states
        default_states = ['Uttar Pradesh', 'Maharashtra', 'Karnataka'] if all(s in all_states for s in ['Uttar Pradesh', 'Maharashtra', 'Karnataka']) else all_states[:3]
        selected_states = st.sidebar.multiselect("Select States", all_states, default=default_states)
    
    with st.sidebar.expander("⚙️ Advanced Map Settings", expanded=False):
        use_system_loc = st.checkbox("Use System Location", value=False, help="Center map on your current real location")
        map_style = st.selectbox("Map Style", ["Streets", "Dark", "Light", "Satellite"], index=1)
    
    # Activity View Selection
    st.sidebar.markdown("---")
    activity_view = st.sidebar.radio(
        "Select Service to Analyze",
        ["Total Updates", "Biometric Updates", "Demographic Updates", "New Enrolments"],
        index=0,
        help="Switch between different types of Aadhaar activity on the map."
    )

    with st.sidebar.expander("💡 Quick Tutorial"):
        st.write("""
        1. **Select States** to initialize the view.
        2. **Toggle Services** to see specific surcharges.
        3. **Hover on Bubbles** for district stats.
        4. **Hover on Arcs** to see migration paths.
        """)
    
    # Filter Data (Only if states are selected)
    if selected_states:
        mask_pulse = (df_pulse['date'] >= pd.to_datetime(start_date)) & (df_pulse['date'] <= pd.to_datetime(end_date))
        mask_pulse = mask_pulse & (df_pulse['state'].isin(selected_states))
        filtered_pulse = df_pulse[mask_pulse]
        
        mask_mig = (df_migration['date'] >= pd.to_datetime(start_date)) & (df_migration['date'] <= pd.to_datetime(end_date))
        mask_mig = mask_mig & (df_migration['source_state'].isin(selected_states) | df_migration['dest_state'].isin(selected_states))
        filtered_mig = df_migration[mask_mig]
    else:
        filtered_pulse = pd.DataFrame(columns=df_pulse.columns)
        filtered_mig = pd.DataFrame(columns=df_migration.columns)
        st.info("💡 Please select one or more states from the sidebar to visualize national activity and migration flows.")

    # Sample data if too large
    MAX_DISPLAY_ROWS = 3000
    if len(filtered_pulse) > MAX_DISPLAY_ROWS:
        display_pulse_sampled = filtered_pulse.sample(n=MAX_DISPLAY_ROWS, random_state=42)
        st.sidebar.warning(f"Displaying a sample of {MAX_DISPLAY_ROWS:,} activity points.")
    else:
        display_pulse_sampled = filtered_pulse.copy()

    # Mapping logic for selected activity
    metric_label = activity_view
    if not display_pulse_sampled.empty:
        if activity_view == "Biometric Updates":
            display_pulse_sampled['volume'] = display_pulse_sampled['bio_age_5_17'] + display_pulse_sampled['bio_age_17_']
        elif activity_view == "Demographic Updates":
            display_pulse_sampled['volume'] = display_pulse_sampled['demo_age_5_17'] + display_pulse_sampled['demo_age_17_']
        elif activity_view == "New Enrolments":
            display_pulse_sampled['volume'] = display_pulse_sampled['total_enrolments']
        else:
            display_pulse_sampled['volume'] = display_pulse_sampled['total_updates']
    else:
        display_pulse_sampled['volume'] = pd.Series(dtype='float64')

    # Final rename for tooltip consistency
    display_pulse_sampled = display_pulse_sampled.rename(columns={'district': 'location'})
    
    # Group Migration Arcs by Route
    if not filtered_mig.empty:
        route_mig = filtered_mig.groupby([
            'source_district', 'dest_district', 'source_state', 'dest_state',
            'source_lat', 'source_lon', 'dest_lat', 'dest_lon'
        ]).agg({
            'count': 'sum',
            'is_anomaly': 'max'
        }).reset_index()
        route_mig = route_mig.rename(columns={'source_district': 'location', 'count': 'volume'})
    else:
        route_mig = pd.DataFrame()

    # KPIs
    st.markdown("### 📊 Live Performance Summary")
    c1, c2, c3 = st.columns(3)
    # Calculate volume sum for the selected view
    view_total = display_pulse_sampled['volume'].sum() if not display_pulse_sampled.empty else 0
    total_enr = filtered_pulse['total_enrolments'].sum() if not filtered_pulse.empty else 0
    total_mig = filtered_mig['count'].sum() if not filtered_mig.empty else 0
    
    # Update KPI metric label based on view
    c1.metric(f"Active View: {metric_label}", f"{view_total:,.0f}")
    c2.metric("Total Enrolments (Pan-India)", f"{total_enr:,.0f}")
    c3.metric("Migrations Tracked", f"{total_mig:,.0f}")

    # Tabs
    tab_map, tab_trends, tab_anomalies, tab_predictions, tab_ai = st.tabs([
        "Live Map", "Trends", "Anomalies", "Predictions", "AI Assistant 🤖"
    ])

    with tab_map:
        st.markdown('<div class="map-header">🛰️ National Aadhaar Activity Map</div>', unsafe_allow_html=True)
        
        # Map Legend
        st.markdown("""
        <div class="legend-card">
            <b>Legend:</b> <br/>
            🔵 <b>Bubbles:</b> Local activity density (Size = Volume) <br/>
            ⚡ <b>Arcs:</b> Inter-district migration (Color: <b>Cyan</b>=Normal, <b>Red</b>=Anomalous)
        </div>
        """, unsafe_allow_html=True)
        
        # --- MapTiler Configuration ---
        maptiler_api_key = os.environ.get("MAPTILER_API_KEY")
        
        if not maptiler_api_key:
            if 'maptiler_key' not in st.session_state:
                st.session_state.maptiler_key = ""
            
            user_key = st.sidebar.text_input("MapTiler API Key", 
                                           type="password", 
                                           value=st.session_state.maptiler_key,
                                           help="Enter your MapTiler API key for premium map styles.")
            
            if user_key:
                st.session_state.maptiler_key = user_key
                maptiler_api_key = user_key
        
        # --- Map Style Options ---
        map_style_option = st.sidebar.selectbox(
            "Map Style",
            ["Streets", "Dark", "Satellite", "Hybrid", "Topo"],
            index=0,
            help="Choose the map background style"
        )
        
        # Determine Map Style using MapTiler
        if maptiler_api_key:
            style_map = {
                "Streets": f"https://api.maptiler.com/maps/streets/style.json?key={maptiler_api_key}",
                "Dark": f"https://api.maptiler.com/maps/dataviz-dark/style.json?key={maptiler_api_key}",
                "Satellite": f"https://api.maptiler.com/maps/satellite/style.json?key={maptiler_api_key}",
                "Hybrid": f"https://api.maptiler.com/maps/hybrid/style.json?key={maptiler_api_key}",
                "Topo": f"https://api.maptiler.com/maps/topo-v2/style.json?key={maptiler_api_key}",
            }
            map_style = style_map.get(map_style_option, style_map["Dark"])
        else:
            # Fallback to free CARTO style if no MapTiler key
            map_style = pdk.map_styles.CARTO_DARK
            st.sidebar.warning("Add MapTiler API Key for premium map styles.")

        if not filtered_pulse.empty or not filtered_mig.empty:
            # Pre-calculate radius for pulse (log scale for visibility)
            import numpy as np
            if not display_pulse_sampled.empty:
                # Use a slightly higher multiplier for better visibility of split metrics
                display_pulse_sampled['pulse_radius'] = np.log1p(display_pulse_sampled['volume']) * 3.5
            else:
                display_pulse_sampled['pulse_radius'] = 0

            # --- Dynamic View State Calculation ---
            if use_system_loc:
                center_lat, center_lon = get_system_location()
                zoom = 9 # Closer look if using system location
            else:
                # Calculate center from pulse data primarily
                if not display_pulse_sampled.empty:
                    all_lats = display_pulse_sampled['latitude']
                    all_lons = display_pulse_sampled['longitude']
                elif not filtered_mig.empty:
                    all_lats = pd.concat([filtered_mig['source_lat'], filtered_mig['dest_lat']])
                    all_lons = pd.concat([filtered_mig['source_lon'], filtered_mig['dest_lon']])
                else:
                    all_lats, all_lons = pd.Series([20.5937]), pd.Series([78.9629])
                    
                center_lat = all_lats.mean()
                center_lon = all_lons.mean()
                
                # Calculate zoom based on data spread
                lat_spread = all_lats.max() - all_lats.min()
                lon_spread = all_lons.max() - all_lons.min()
                max_spread = max(lat_spread, lon_spread)
                
                # Approximate zoom level
                if max_spread < 2: zoom = 7
                elif max_spread < 5: zoom = 6
                elif max_spread < 10: zoom = 5
                else: zoom = 4

            # --- Layers ---
            layers = []
            
            # 1. Pulse Scatterplot Layer
            if not display_pulse_sampled.empty:
                pulse_layer = pdk.Layer(
                    "ScatterplotLayer",
                    data=display_pulse_sampled,
                    get_position=["longitude", "latitude"],
                    get_radius="pulse_radius", 
                    radius_units="pixels",
                    get_fill_color=[0, 255, 255, 120], # Cyan
                    radius_min_pixels=3,
                    radius_max_pixels=25,
                    pickable=True,
                    auto_highlight=True,
                )
                layers.append(pulse_layer)

            # 2. Migration Arc Layer (using grouped route_mig)
            if not route_mig.empty:
                # Optimized arc colors
                route_mig['arc_color_r'] = route_mig['is_anomaly'].apply(lambda x: 255 if x else 0)
                route_mig['arc_color_g'] = route_mig['is_anomaly'].apply(lambda x: 100 if x else 255)
                route_mig['arc_color_b'] = route_mig['is_anomaly'].apply(lambda x: 0 if x else 255)

                arc_layer = pdk.Layer(
                    "ArcLayer",
                    data=route_mig,
                    get_source_position=["source_lon", "source_lat"],
                    get_target_position=["dest_lon", "dest_lat"],
                    get_source_color=[0, 255, 255, 120],  # Cyan start
                    get_target_color=["arc_color_r", "arc_color_g", "arc_color_b", 180],
                    get_width="2 + (volume / 20)", # Slightly thicker for visibility
                    pickable=True,
                    auto_highlight=True,
                )
                layers.append(arc_layer)
            
            tooltip_html = {
                "html": f"<b>Location:</b> {{location}}<br/>"
                        f"<b>{metric_label}:</b> {{volume}} events",
                "style": {"backgroundColor": "black", "color": "white"}
            }

            # --- View State (Dynamic) ---
            view_state = pdk.ViewState(
                latitude=center_lat,
                longitude=center_lon,
                zoom=zoom,
                pitch=45,
                bearing=0,
            )
            
            # --- Render ---
            deck_args = {
                "map_style": map_style,
                "initial_view_state": view_state,
                "layers": layers,
                "tooltip": tooltip_html
            }

            st.pydeck_chart(pdk.Deck(**deck_args))
            
            # Legend
            st.markdown("""
                <div style='text-align: center; color: #ccc; margin-top: 10px;'>
                    <span style='color: #00FFFF;'>●</span> District Activity (Size = Update Volume)
                </div>
            """, unsafe_allow_html=True)
            
        else:
            st.info("No data available for the selected filters.")

    with tab_trends:
        st.subheader("Aadhaar Activity Trends")
        if not filtered_pulse.empty:
            # We need to apply the same metric logic to the trend data
            pulse_trend = filtered_pulse.copy()
            if activity_view == "Biometric Updates":
                pulse_trend['volume'] = pulse_trend['bio_age_5_17'] + pulse_trend['bio_age_17_']
            elif activity_view == "Demographic Updates":
                pulse_trend['volume'] = pulse_trend['demo_age_5_17'] + pulse_trend['demo_age_17_']
            elif activity_view == "New Enrolments":
                pulse_trend['volume'] = pulse_trend['total_enrolments']
            else:
                pulse_trend['volume'] = pulse_trend['total_updates']

            trend_data = pulse_trend.groupby('date')['volume'].sum().reset_index()
            fig = px.line(trend_data, x='date', y='volume', 
                         title=f"National {metric_label} Over Time",
                         labels={'volume': 'Events', 'date': 'Date'})
            st.plotly_chart(fig, use_container_width=True)

    with tab_anomalies:
        st.subheader("Detected Anomalies")
        # Check both datasets for anomalies
        anomalous_events = []
        if 'is_anomaly' in filtered_mig.columns:
            anomalies = filtered_mig[filtered_mig['is_anomaly']]
            if not anomalies.empty:
                st.error(f"⚠️ Detected {len(anomalies)} potential anomalous migration events.")
                st.dataframe(
                    anomalies[['date', 'source_district', 'dest_district', 'count', 'anomaly_score']].sort_values('anomaly_score', ascending=False),
                    use_container_width=True
                )
            else:
                st.success("No critical migration anomalies detected.")
        else:
            st.info("Anomaly detection is scanning migration flows...")

    with tab_predictions:
        st.subheader("🔮 Predictive Analytics & AI Insights")
        st.markdown("""
            This module uses **Prophet Time-Series models** to analyze historical enrollment and update patterns 
            to project future trends and provide actionable insights.
        """)
        
        if not filtered_pulse.empty:
            # Prepare data for forecast based on selected metric
            df_for_pred = filtered_pulse.copy()
            target_col = 'total_updates' # Default
            
            if activity_view == "Biometric Updates":
                df_for_pred['volume'] = df_for_pred['bio_age_5_17'] + df_for_pred['bio_age_17_']
                target_col = 'biometric_updates'
            elif activity_view == "Demographic Updates":
                df_for_pred['volume'] = df_for_pred['demo_age_5_17'] + df_for_pred['demo_age_17_']
                target_col = 'demographic_updates'
            elif activity_view == "New Enrolments":
                df_for_pred['volume'] = df_for_pred['total_enrolments']
                target_col = 'new_enrolments'
            else:
                df_for_pred['volume'] = df_for_pred['total_updates']
                target_col = 'total_updates'

            with st.spinner(f"Analyzing trends for {activity_view}..."):
                forecast, model = get_forecast(df_for_pred, target_col='volume')
            
            if not forecast.empty:
                # Layout for Forecast Chart and Analysis
                col_chart, col_insights = st.columns([2, 1])
                
                with col_chart:
                    st.markdown(f"#### {activity_view} Forecast (Next 30 Days)")
                    
                    # Create a combined chart with historical and forecast
                    # Historical data
                    hist_data = df_for_pred.groupby('date')['volume'].sum().reset_index()
                    hist_data.rename(columns={'date': 'ds', 'volume': 'y'}, inplace=True)
                    
                    fig_pred = go.Figure()
                    
                    # Confidence Interval (Shaded area)
                    fig_pred.add_trace(go.Scatter(
                        x=pd.concat([forecast['ds'], forecast['ds'][::-1]]),
                        y=pd.concat([forecast['yhat_upper'], forecast['yhat_lower'][::-1]]),
                        fill='toself',
                        fillcolor='rgba(0, 255, 255, 0.1)',
                        line=dict(color='rgba(255,255,255,0)'),
                        hoverinfo="skip",
                        showlegend=True,
                        name="Confidence Interval"
                    ))
                    
                    # Predicted line
                    fig_pred.add_trace(go.Scatter(
                        x=forecast['ds'], y=forecast['yhat'],
                        name="Predicted",
                        line=dict(color='cyan', width=2, dash='dash')
                    ))
                    
                    # Historical line
                    fig_pred.add_trace(go.Scatter(
                        x=hist_data['ds'], y=hist_data['y'],
                        name="Historical",
                        line=dict(color='#ff4b4b', width=2)
                    ))
                    
                    fig_pred.update_layout(
                        template='plotly_dark',
                        xaxis_title="Date",
                        yaxis_title="Volume",
                        hovermode="x unified",
                        margin=dict(l=0, r=0, t=20, b=0),
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                    )
                    st.plotly_chart(fig_pred, use_container_width=True)
                
                with col_insights:
                    st.markdown("#### 📝 Detailed Analysis")
                    insights = generate_forecast_insights(forecast, model, df_for_pred, target_col)
                    
                    for insight in insights:
                        st.markdown(f"🔹 {insight}")
                    
                    st.info("💡 **Planning Tip:** Use these projections to optimize resource allocation at Aadhaar centers during predicted peak periods.")
            else:
                st.warning("Not enough historical data in the selected range to generate a reliable forecast. Try selecting a broader date range or more states.")
        else:
            st.info("Please select data filters to see predictions.")

    with tab_ai:
        st.subheader("🤖 Aadhaar AI Assistant")
        st.markdown("""
            Ask questions about current trends, anomalies, or general Aadhaar statistics. 
            The assistant has access to the **currently filtered data** context.
        """)

        # Initialize Chatbot and Context
        ollama = OllamaClient()
        
        # Prepare context for the LLM
        context_summary = {
            "view": activity_view,
            "states": selected_states,
            "date_range": [start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')],
            "total_volume": float(view_total),
            "enrolments": float(total_enr),
            "migrations": float(total_mig),
            "top_districts": display_pulse_sampled.groupby('location')['volume'].sum().nlargest(3).to_dict() if not display_pulse_sampled.empty else {}
        }
        
        system_prompt = f"""
        You are the Aadhaar Pulse AI, a specialist in analyzing Aadhaar demographic and enrollment data.
        You are helping a government official understand the data displayed on their dashboard.
        
        Current Dashboard Context:
        - Analyzing: {context_summary['view']}
        - Selected States: {', '.join(context_summary['states'])}
        - Date Range: {context_summary['date_range'][0]} to {context_summary['date_range'][1]}
        - Overall Stats for current filters:
            * Total {context_summary['view']} volume: {context_summary['total_volume']:,}
            * Total Enrolments: {context_summary['enrolments']:,}
            * Migration Flows: {context_summary['migrations']:,}
            * Top Districts (by volume): {', '.join([f"{k} ({v:,.0f})" for k, v in context_summary['top_districts'].items()])}
        
        Provide concise, data-driven answers. If the data doesn't support a specific claim, be honest.
        Use Markdown formatting for better readability.
        """

        # Chat interface
        if "messages" not in st.session_state:
            st.session_state.messages = []

        # Display history
        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])

        # Chat input
        if prompt := st.chat_input("Ask about the data..."):
            st.session_state.messages.append({"role": "user", "content": prompt})
            with st.chat_message("user"):
                st.markdown(prompt)

            with st.chat_message("assistant"):
                message_placeholder = st.empty()
                full_response = ""
                
                # Prepare messages for Ollama (System + Chat History)
                ollama_messages = [{"role": "system", "content": system_prompt}]
                ollama_messages.extend([
                    {"role": m["role"], "content": m["content"]}
                    for m in st.session_state.messages[-5:] # Last 5 turns for context
                ])
                
                with st.spinner("Analyzing..."):
                    # Use streaming for better UX
                    response_gen = ollama.chat(ollama_messages, stream=True)
                    
                    if isinstance(response_gen, str) and response_gen.startswith("Error:"):
                        st.error("⚠️ **Ollama AI Assistant is currently unavailable on the web.**")
                        st.info("To use the AI Assistant, please run the app locally with Ollama installed.")
                        full_response = response_gen
                    else:
                        for chunk in response_gen:
                            full_response += chunk
                            message_placeholder.markdown(full_response + "▌")
                        message_placeholder.markdown(full_response)
                
            if not full_response.startswith("Error:"):
                st.session_state.messages.append({"role": "assistant", "content": full_response})

        if st.sidebar.button("Clear Chat History", key="clear_chat"):
            st.session_state.messages = []
            st.rerun()

if __name__ == "__main__":
    main()
