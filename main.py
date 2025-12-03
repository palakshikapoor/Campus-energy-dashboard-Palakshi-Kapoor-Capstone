import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# -----------------------------
# TASK 1: DATA INGESTION
# -----------------------------
def load_and_combine_data():
    folder = Path("data")
    all_files = folder.glob("*.csv")

    df_list = []

    for file in all_files:
        try:
            # Read CSV with BOM handling
            df = pd.read_csv(file, encoding="utf-8-sig")

            # Standardize column names (strip spaces & lowercase)
            df.columns = df.columns.str.strip().str.lower()

            # Add building name from filename
            df["building"] = file.stem.split("_")[1]

            df_list.append(df)

        except Exception as e:
            print(f"Error loading {file}: {e}")

    df_combined = pd.concat(df_list, ignore_index=True)

    # Convert timestamp column automatically (Excel-style dates)
    df_combined["timestamp"] = pd.to_datetime(df_combined["timestamp"], dayfirst=False)

    return df_combined

# -----------------------------
# TASK 2: AGGREGATIONS
# -----------------------------
def calculate_daily_totals(df):
    return df.resample("D", on="timestamp")["kwh"].sum()

def calculate_weekly_totals(df):
    return df.resample("W", on="timestamp")["kwh"].sum()

def building_summary(df):
    return df.groupby("building")["kwh"].agg(["mean", "min", "max", "sum"])

# -----------------------------
# TASK 3: OOP CLASSES
# -----------------------------
class MeterReading:
    def __init__(self, timestamp, kwh):
        self.timestamp = timestamp
        self.kwh = kwh

class Building:
    def __init__(self, name):
        self.name = name
        self.meter_readings = []

    def add_reading(self, reading):
        self.meter_readings.append(reading)

    def total_consumption(self):
        return sum(r.kwh for r in self.meter_readings)

    def generate_report(self):
        total = self.total_consumption()
        peak = max(self.meter_readings, key=lambda r: r.kwh)
        return f"Building: {self.name}\nTotal Consumption: {total}\nPeak: {peak.kwh} at {peak.timestamp}\n"

class BuildingManager:
    def __init__(self):
        self.buildings = {}

    def add_reading(self, building_name, reading):
        if building_name not in self.buildings:
            self.buildings[building_name] = Building(building_name)
        self.buildings[building_name].add_reading(reading)

# -----------------------------
# TASK 4: VISUALIZATION
# -----------------------------
def generate_dashboard(df):
    fig, axes = plt.subplots(3, 1, figsize=(12, 14))

    # Trend line per building
    for name, group in df.groupby("building"):
        daily = group.resample("D", on="timestamp")["kwh"].sum()
        axes[0].plot(daily.index, daily.values, marker="o", label=name)
    axes[0].set_title("Daily Consumption Trend per Building")
    axes[0].set_ylabel("kWh")
    axes[0].legend()
    axes[0].grid(True)

    # Weekly bar chart (total consumption per building)
    weekly_summary = df.groupby("building")["kwh"].sum()
    axes[1].bar(weekly_summary.index, weekly_summary.values, color=["skyblue","orange","green"])
    axes[1].set_title("Total Consumption per Building (Weekly)")
    axes[1].set_ylabel("kWh")
    axes[1].grid(axis="y")

    # Scatter plot: hourly consumption
    colors = {"A":"red", "B":"blue", "C":"green"}
    for name, group in df.groupby("building"):
        axes[2].scatter(group["timestamp"], group["kwh"], label=name, alpha=0.6)
    axes[2].set_title("Hourly Consumption Scatter")
    axes[2].set_ylabel("kWh")
    axes[2].set_xlabel("Timestamp")
    axes[2].legend()
    axes[2].grid(True)

    plt.tight_layout()
    Path("output").mkdir(exist_ok=True)
    plt.savefig("output/dashboard.png")
    plt.close()
    print("Dashboard saved to output/dashboard.png")

# -----------------------------
# TASK 5: SAVE OUTPUTS
# -----------------------------
def save_outputs(df):
    # Create output folder if it doesn't exist
    Path("output").mkdir(exist_ok=True)

    df.to_csv("output/cleaned_energy_data.csv", index=False)
    building_summary(df).to_csv("output/building_summary.csv")

    total = df["kwh"].sum()
    highest = df.groupby("building")["kwh"].sum().idxmax()
    peak_hour = df.loc[df["kwh"].idxmax(), "timestamp"]

    with open("output/summary.txt", "w") as f:
        f.write(f"Total Campus Consumption: {total}\n")
        f.write(f"Highest Consuming Building: {highest}\n")
        f.write(f"Peak Load Hour: {peak_hour}\n")

    print("Outputs saved in the output/ folder")

# -----------------------------
# MAIN SCRIPT
# -----------------------------
def main():
    df = load_and_combine_data()

    print("\nDaily totals:\n", calculate_daily_totals(df))
    print("\nWeekly totals:\n", calculate_weekly_totals(df))
    print("\nBuilding Summary:\n", building_summary(df))

    generate_dashboard(df)
    save_outputs(df)

if __name__ == "__main__":
    main()

