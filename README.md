# AadhaarPulse 🇮🇳

**AadhaarPulse** is a real-time analytics and anomaly detection dashboard designed to monitor and visualize Aadhaar update trends across India. It leverages synthetic data to simulate migration patterns and uses machine learning to forecast trends and detect anomalies.

## 🚀 Impact
This tool empowers administrators to:
- **Visualize Migration:** Track population movement (e.g., Rural -> Urban) in real-time.
- **Detect Fraud:** Identify unusual spikes in updates at specific pincodes.
- **Forecast Demand:** Predict future update volumes to allocate resources efficiently.

---

## 🛠️ Installation & Setup

### Prerequisites
- Python 3.8 or higher
- `pip` (Python package installer)

### 1. Clone/Navigate to the Repository
```bash
cd aadhaarpulse
```

### 2. Set Up Virtual Environment
It's recommended to use a virtual environment.
```bash
# Create virtual environment
python3 -m venv venv

# Activate (Mac/Linux)
source venv/bin/activate

# Activate (Windows)
venv\Scripts\activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

---

## 🏃‍♂️ How to Run

### 1. Generate Data (First Run Only)
Before running the dashboard, generate the synthetic data:
```bash
python src/generation/mock_generator.py
```
*This will create `data/raw_aadhaar_logs.csv`.*

### 2. Process Data (ETL)
Clean and aggregate the raw data:
```bash
python src/processing/aggregator.py
```
*This will create `data/district_flows.csv`.*

### 3. Launch Dashboard
Start the Streamlit application:
```bash
streamlit run main.py
```

The dashboard will open in your browser at `http://localhost:8501`.

---

## 📂 Project Structure
- `src/generation/`: Scripts for creating mock Aadhaar logs.
- `src/processing/`: ETL logic to aggregate data by district.
- `src/models/`: Forecast (Prophet) and Anomaly Detection (Isolation Forest) models.
- `main.py`: The main Streamlit dashboard application.
