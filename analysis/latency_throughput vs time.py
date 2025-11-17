import pandas as pd
import matplotlib.pyplot as plt

# Load CSV
df = pd.read_csv("data/atlantis_performance_13_Nov.csv")

# Convert created_at to datetime
df['created_at'] = pd.to_datetime(df['created_at'])

# Extract hour of day for x-axis
df['hour'] = df['created_at'].dt.hour + df['created_at'].dt.minute / 60.0  # e.g., 13.5 for 13:30

# Sort by time
df = df.sort_values('created_at')

# Plot Latency vs Hour
plt.figure(figsize=(10, 5))
plt.plot(df['hour'], df['latency'], marker='o', color='tab:blue')
plt.title('Latency vs Hour')
plt.xlabel('Hour of Day')
plt.ylabel('Latency (s)')
plt.grid(True)
plt.tight_layout()
plt.show()

# Plot Throughput vs Hour
plt.figure(figsize=(10, 5))
plt.plot(df['hour'], df['throughput'], marker='o', color='tab:orange')
plt.title('Throughput vs Hour')
plt.xlabel('Hour of Day')
plt.ylabel('Throughput (windows per second)')
plt.grid(True)
plt.tight_layout()
plt.show()

# Optional: basic stats
print("=== Summary Statistics ===")
print(df[['latency', 'throughput']].describe())