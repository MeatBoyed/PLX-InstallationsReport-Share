# %% [markdown]
# # FNO Deliveries Report Overview
# 
# 
# ## **Idea/Notes**
# Instead of doing Each individual day, Try Coupling the days into groups for more clean data represnetation. Example: Group by every 5 days, 10 days, or x Days accross the 2 month period
# 
# ## Insights Drawn out
# 
# ### **Metrics & Values**
# 1. Total Customers (Aquired)  
# 2. Total Customers Online 
# 3. Total Customer Offline  
# 4. Total Customers per Network  
# 
# 1. Total Revenue (Aquired) - 
# 2. Total Revenue for Online Customers -
# 3. Total Revenue for Offline Customers - 
# 3. Total Revenue per Network - 
# 
# ### **Visualization/Chart**
# 1. Total Customer Growth (All networks) - Line Chart 
# 2. Customer Growth per Network - LineChart / Count Chart (Vertical barchart) - Combination of Culumlative/Totaling of Customers per Day, per Network 
# 3. Total Customers Online vs Offline (Graph) 
# 4. Customer Distribution per Network - Pie Chart  
# 
# 1. Total Revenue Growth (All networks) - Line Chart - 
# 2. Revenue Growth per Network - Line Chart - 
# 3. Revenue Distribution per Network - Pie Chart / Bar Chart - 

# %%
# Import Packages
import plotly
import numpy as np
import pandas as pd
import seaborn as sns
import plotly.express as px
import matplotlib.pyplot as plt
plotly.offline.init_notebook_mode(connected=True)

# %% [markdown]
# ### Importing Dataset & Cleaning data for Aggregation

# %%
ReportingDate = "04-14"

# %%
# Load Data
folderPath = f"./Reports/{ReportingDate}"
file_path = f"{folderPath}/FNO-{ReportingDate}.csv"
# xls = pd.ExcelFile(file_path)
# df = pd.read_excel(xls, sheet_name="Router Deliveries ")
# raw_df = pd.read_csv(file_path)
raw_df = pd.read_csv(f"./FNO-{ReportingDate}.csv")
# raw_df

# %%
# Clean & Format Columns
df = raw_df.copy()
# df = raw_df.iloc[1:].reset_index(drop=True) # Remove the 1st row
# df = df.drop(columns=["Unnamed: 0", "Notes", "Client Name & Surname"]) # Remove Un-necessary columns
# df.columns = [
#     "RouterCount", "Date", "Address", 
#     "Phone", "RouterSerial", "MAC", "Network", 
#     "RawMonthlyRevenue", "ClientOnline"
# ] # Rename columns
df.columns = [
    "Date",  "RouterSerial", "MAC", "Network", 
    "RawMonthlyRevenue", "ClientOnline"
] # Rename columns
df

# %%
df["Date"] = pd.to_datetime(df["Date"], errors='coerce') # Convert string dates to DateTime
df["FormattedDate"] = df["Date"].dt.strftime("%d-%m") # Format the date (Day - Month)
df["MonthlyRevenue"] = (
    df["RawMonthlyRevenue"]
    .str.replace("R", "")  # Remove "R"
    .str.replace(",", "")  # Remove commas
    .replace(r"^\s*-\s*$", np.nan, regex=True)  # Replace " -   " with NaN
    .astype(float)         # Convert to float (invalid values become NaN)
)
df

# %% [markdown]
# ## Calculation and Aggregation of Data

# %% [markdown]
# ### Metrics & Values

# %% [markdown]
# #### Customers

# %%
total_customers = df["Date"].size
total_customers_online = df[df["ClientOnline"] == True].shape[0]
total_customers_offline = df[df["ClientOnline"] == False].shape[0]

# Total Customers per Network
customer_count_per_network = df.groupby("Network").size().reset_index(name="TotalCustomers") # DataFrame - Combined Breakdown
# Raw Total Customers per Network Individual Values 
customer_count_per_network_raw = [] # List of Dict {Network: value, }
for index, row in customer_count_per_network.iterrows():
    customer_count_per_network_raw.append({
        "network": row["Network"],
        "total": row["TotalCustomers"]
    })
    
print(f"Total Customers: {total_customers}")
print(f"Total Customers Online: {total_customers_online}")
print(f"Total Customers Offline: {total_customers_offline}")
print(f"Customer Count Per Nework: \n{display(customer_count_per_network)}")

# %% [markdown]
# #### Revenue

# %%
# 1. Total Revenue (Acquired)
total_revenue_acquired = df["MonthlyRevenue"].sum()

# 2. Total Revenue for Online Customers
total_revenue_online = df[df["ClientOnline"] == True]["MonthlyRevenue"].sum()

# 3. Total Revenue for Offline Customers
total_revenue_offline = df[df["ClientOnline"] == False]["MonthlyRevenue"].sum()

# 4. Total Revenue per Network
total_revenue_per_network = df.groupby("Network")["MonthlyRevenue"].sum().reset_index()

# Print results
print(f"Total Revenue (Acquired): {total_revenue_acquired}")
print(f"Total Revenue for Online Customers: {total_revenue_online}")
print(f"Total Revenue for Offline Customers: {total_revenue_offline}")
# print("Total Revenue per Network:")
# print(total_revenue_per_network)
print(f"Total Revnuve per Network: \n{display(total_revenue_per_network)}")

# %% [markdown]
# #### Aggregation
# 1. Total Revenue Growth (All networks) - Line Chart - 
# 2. Revenue Growth per Network - Line Chart - 
# 3. Revenue Distribution per Network - Pie Chart / Bar Chart - 

# %%
# Customer
customer_count_per_day = df.groupby("FormattedDate").size().reset_index(name="NewCustomers")  # Count New Customers per Day across all Networks - usefull for trend analysis
customer_count_per_day["TotalCustomers"] = customer_count_per_day["NewCustomers"].cumsum() # Customer Growth (Totaling/Cumulative total) per Day

# Revenue
# 1. Total Revenue Growth (All Networks)
revenue_growth_all_networks = df.groupby("FormattedDate")["MonthlyRevenue"].sum().reset_index()
revenue_growth_all_networks["Cumulative Revenue (All Networks)"] = revenue_growth_all_networks["MonthlyRevenue"].cumsum()
revenue_growth_all_networks = revenue_growth_all_networks.rename(columns={"MonthlyRevenue": "Daily Revenue (All Networks)"})

# 2. Revenue Growth per Network
revenue_growth_per_network = df.groupby(["FormattedDate", "Network"])["MonthlyRevenue"].sum().reset_index()
revenue_growth_per_network["Cumulative Revenue (Per Network)"] = revenue_growth_per_network.groupby("Network")["MonthlyRevenue"].cumsum()
revenue_growth_per_network = revenue_growth_per_network.rename(columns={"MonthlyRevenue": "Daily Revenue (Per Network)"})

# 3. Revenue Distribution per Network
revenue_distribution_per_network = df.groupby("Network")["MonthlyRevenue"].sum().reset_index()
revenue_distribution_per_network = revenue_distribution_per_network.rename(columns={"MonthlyRevenue": "Total Revenue (Per Network)"})

# 4. Total Revenue Online vs Offline
revenue_online_vs_offline = df.groupby(["FormattedDate", "ClientOnline"])["MonthlyRevenue"].sum().reset_index()
revenue_online_vs_offline = revenue_online_vs_offline.rename(columns={"MonthlyRevenue": "Total Revenue (Online vs Offline)"})

# Merge all DataFrames into one
# Start with the base DataFrame (revenue_growth_all_networks)
merged_df = revenue_growth_all_networks
# Merge revenue_growth_per_network
merged_df = pd.merge(merged_df, revenue_growth_per_network, on=["FormattedDate"], how="left")
# Merge revenue_distribution_per_network
merged_df = pd.merge(merged_df, revenue_distribution_per_network, on=["Network"], how="left")
# Merge revenue_online_vs_offline
merged_df = pd.merge(merged_df, revenue_online_vs_offline, on=["FormattedDate"], how="left")

# Print the final merged DataFrame

# print(f"Customer Count Per Day: \n{display(customer_count_per_day)}")
# merged_df


# %% [markdown]
# ## Visualization of Data

# %% [markdown]
# #### Customer Graphs

# %%
# Customer Growth accross all networks
sns.lineplot(data=customer_count_per_day, x='FormattedDate', y='TotalCustomers')
plt.xticks(rotation=45)
plt.title('Customer Growth Across all Networks')
plt.show()

# %%
# Customers Online vs Offline
data = [total_customers_online, total_customers_offline] 
keys = ["Online", "Offline"] 
palette_color = sns.color_palette('bright') 
plt.pie(data, labels=keys, colors=palette_color, autopct='%.0f%%') 
plt.title("Online vs Offline Customers")
plt.show() 

# %%
# Total Customers Per Network

# ax = sns.barplot(data=customer_count_per_network, x='Network', y='Total Customers', errorbar=None)
# vp_seaborn_show_values(ax)
# plt.legend(loc='best')
# plt.title('Total Customers Per Network')
# plt.xticks(rotation=30, ha="right")
# plt.grid(False)
# plt.show()

fig = px.bar(customer_count_per_network, x='Network', y='TotalCustomers', color='Network', title="Total Customers by Network")
fig.update_xaxes(categoryorder='total descending')
fig.show()

# %%
# Distrubution of Customers by Network
fig = px.pie(customer_count_per_network, values='TotalCustomers', names='Network', color='Network', title="Total Customers by Network")
fig.show()

# %%
# Count customers per network and month
daily_growth_per_network = df.groupby(["FormattedDate", "Network"]).size().unstack(fill_value=0)
cumulative_growth_per_network = daily_growth_per_network.cumsum()
# Convert index to column for Plotly
cumulative_growth_per_network = cumulative_growth_per_network.reset_index()

# Plot using Plotly
fig = px.line(
    cumulative_growth_per_network, 
    x="FormattedDate", 
    y=cumulative_growth_per_network.columns[1:],  # Select all networks as Y values
    labels={"value": "Total Customers", "Formatted Date": "Date"},
    title="Cumulative Customer Growth per Network"
)
fig.update_xaxes(type="category")
fig.show()

# %% [markdown]
# #### Revenue Graphs

# %%


# %%
# Visual Python: Visualization > Seaborn
sns.lineplot(data=revenue_growth_all_networks, x='FormattedDate', y='Cumulative Revenue (All Networks)')
plt.xticks(rotation=45)
plt.title('Total Revenue Growth')
plt.xlabel('Date (Day/Month)')
plt.ylabel('Total Revenue')
plt.show()

# %%
# Visual Python: Visualization > Seaborn
sns.lineplot(data=revenue_growth_all_networks, x='FormattedDate', y='Daily Revenue (All Networks)')
plt.xticks(rotation=45)
plt.title('Daily Revenue Growth')
plt.xlabel('Date (Day/Month)')
plt.ylabel('Revenue')
plt.show()

# %%
# Visual Python: Visualization > Plotly
fig = px.bar(revenue_distribution_per_network, x='Network', y='Total Revenue (Per Network)', title='Total Revenue per Network', labels={ 'Total Revenue (Per Network)': 'Total Revenue' })
fig.update_xaxes(categoryorder='total descending')
fig.show()

# %%
# Revenue Distribution per Newtork
fig = px.pie(revenue_distribution_per_network, values='Total Revenue (Per Network)', names='Network', color='Network', title="Total Revenue per Network")
fig.show()

# %%
# Group by FormattedDate and Network, then sum the MonthlyRevenue
daily_revenue_per_network = df.groupby(["FormattedDate", "Network"])["MonthlyRevenue"].sum().unstack(fill_value=0)

# Calculate cumulative revenue for each network
cumulative_revenue_per_network = daily_revenue_per_network.cumsum()

# Convert index to column for Plotly
cumulative_revenue_per_network = cumulative_revenue_per_network.reset_index()

# Plot using Plotly
fig = px.line(
    cumulative_revenue_per_network, 
    x="FormattedDate", 
    y=cumulative_revenue_per_network.columns[1:],  # Select all networks as Y values
    labels={"value": "Total Revenue", "FormattedDate": "Date"},
    title="Cumulative Revenue Growth per Network"
)
fig.update_xaxes(type="category")
fig.show()

# %%
# Visual Python: Visualization > Seaborn
import numpy as np
def vp_seaborn_show_values(axs, precision=1, space=0.01):
    pstr = '{:.' + str(precision) + 'f}'
    
    def _single(ax):
        # check orient
        orient = 'v'
        if len(ax.patches) == 1:
            # check if 0
            if ax.patches[0].get_x() == 0:
                orient = 'h'
        else:
            # compare 0, 1 patches
            p0 = ax.patches[0]
            p1 = ax.patches[1]
            if p0.get_x() == p1.get_x():
                orient = 'h'
                
        if orient == 'v':
            for p in ax.patches:
                _x = p.get_x() + p.get_width() / 2
                _y = p.get_y() + p.get_height() + (p.get_height()*space)
                if not np.isnan(_x) and not np.isnan(_y):
                    value = pstr.format(p.get_height())
                    ax.text(_x, _y, value, ha='center') 
        elif orient == 'h':
            for p in ax.patches:
                _x = p.get_x() + p.get_width() + (space - 0.01)
                _y = p.get_y() + p.get_height() / 2
                if not np.isnan(_x) and not np.isnan(_y):
                    value = pstr.format(p.get_width())
                    ax.text(_x, _y, value, ha='left')

    if isinstance(axs, np.ndarray):
        for idx, ax in np.ndenumerate(axs):
            _single(ax)
    else:
        _single(axs)

# %%
# Clean up the data
# df_cleaned = df.iloc[3:].reset_index(drop=True)
# df_cleaned.columns = [
#     "Index", "Router Count", "Date", "Client Name", "Address", "Contact No",
#     "Router Serial No", "Router MAC", "Network", "Client Online", "Notes"
# ]

# Clean data
# df_cleaned = df_cleaned.drop(columns=["Index"])
# df_cleaned["Date"] = pd.to_datetime(df_cleaned["Date"], errors='coerce')
# df_cleaned = df_cleaned.dropna(subset=["Date"])
# df_cleaned["Year-Month"] = df_cleaned["Date"].dt.to_period("M")
# df_cleaned["FormattedDate"] = df_cleaned["Date"].dt.strftime("%d-%m") # Format the date (Day - Month)


df_cleaned

# %% [markdown]
# CREATE TABLE public.fno_data
# (
#     id uuid NOT NULL,
#     "RouteCount" bigint,
#     "Date" date NOT NULL,
#     "NameSurname" character varying NOT NULL,
#     "Address" character varying,
#     "Phone" character varying NOT NULL,
#     "RouterSerialNo" character varying,
#     "RouterMAC" character varying,
#     "Network" character varying,
#     "RawMonthlyRevenue" character varying,
#     "ClientOnline" boolean NOT NULL,
#     "Notes" character varying,
#     "MonthlyRevenue" money NOT NULL,
#     PRIMARY KEY (id)
# );

# %% [markdown]
# ## Conversion!!
# Ensure UUIDs are defined
# ```
# # Add a new 'id' column with unique UUIDs
# df["id"] = [uuid.uuid4() for _ in range(len(df))]
# 
# # Ensure the UUIDs are strings for PostgreSQL compatibility
# df["id"] = df["id"].astype(str)
# ```
# 
# Ensure There are No NaNs!!!
# 
# Ensure df is in right table column order
# ```
# # Define correct column order as per SQL table (Fixed RouterCount)
# correct_order = [
#     "id", "RouterCount", "Date", "NameSurname", "Address", "Phone",
#     "RouterSerialNo", "RouterMAC", "Network", "RawMonthlyRevenue",
#     "ClientOnline", "Notes", "MonthlyRevenue"
# ]
# 
# # Reorder DataFrame columns
# df = df[correct_order]
# ```
# 
# Generate SQL Statement
# ```
# # Generate SQL INSERT statements
# sql_statements = [
#     f"INSERT INTO {table_name} ({', '.join(['id'] + [f'\"{col}\"' for col in df.columns if col != 'id'])}) "
#     f"VALUES ({repr(row[0])}, {', '.join(map(repr, row[1:]))});"
#     for row in df.itertuples(index=False, name=None)
# ]
# 
# # Print the SQL statements
# for sql in sql_statements:
#     print(sql)
#     ```

# %% [markdown]
# To accommodate the new columns in the `FNOData` table and update the views for aggregations, we need to modify the schema and ensure the views reflect the required metrics. Below is the updated schema and the corresponding views.
# 
# ---
# 
# ### **Updated `FNOData` Table**
# 
# The `FNOData` table will now store the following columns:
# 
# | Column Name           | Data Type       | Description                                      |
# |-----------------------|-----------------|--------------------------------------------------|
# | `id`                  | `SERIAL`        | Primary key (auto-incrementing).                |
# | `raw_data_id`         | `INT`           | Foreign key referencing `RawData.id`.           |
# | `router_count`        | `INT`           | Number of routers.                              |
# | `date`                | `DATE`          | Date of the record.                             |
# | `client_name`         | `VARCHAR(100)`  | Client's full name.                             |
# | `contact_no`          | `VARCHAR(20)`   | Client's contact number.                        |
# | `router_serial_no`    | `VARCHAR(50)`   | Router's serial number.                         |
# | `router_mac`          | `VARCHAR(50)`   | Router's MAC address.                           |
# | `network`             | `VARCHAR(50)`   | Network associated with the client.             |
# | `monthly_revenue`     | `DECIMAL(10, 2)`| Monthly revenue from the client.                |
# | `client_online`       | `BOOLEAN`       | Whether the client is online.                   |
# | `notes`               | `TEXT`          | Additional notes.                               |
# 
# **SQL:**
# ```sql
# CREATE TABLE FNOData (
#     id SERIAL PRIMARY KEY,
#     raw_data_id INT REFERENCES RawData(id) ON DELETE CASCADE,
#     router_count INT NOT NULL,
#     date DATE NOT NULL,
#     client_name VARCHAR(100) NOT NULL,
#     contact_no VARCHAR(20) NOT NULL,
#     router_serial_no VARCHAR(50) NOT NULL,
#     router_mac VARCHAR(50) NOT NULL,
#     network VARCHAR(50) NOT NULL,
#     monthly_revenue DECIMAL(10, 2) NOT NULL,
#     client_online BOOLEAN NOT NULL,
#     notes TEXT
# );
# ```
# 
# ---
# 
# ### **Views for Aggregations**
# 
# #### **1. `TotalCustomersView`**
# Provides the total number of customers (acquired, online, and offline).
# 
# ```sql
# CREATE OR REPLACE VIEW TotalCustomersView AS
# SELECT
#     COUNT(DISTINCT client_name) AS total_customers,
#     COUNT(DISTINCT CASE WHEN client_online = TRUE THEN client_name END) AS total_online_customers,
#     COUNT(DISTINCT CASE WHEN client_online = FALSE THEN client_name END) AS total_offline_customers
# FROM FNOData
# WHERE date BETWEEN :start_date AND :end_date;
# ```
# 
# **Usage:**
# ```sql
# SELECT * FROM TotalCustomersView WHERE :start_date = '2023-01-01' AND :end_date = '2023-12-31';
# ```
# 
# ---
# 
# #### **2. `TotalRevenueView`**
# Provides the total revenue (acquired, online, and offline).
# 
# ```sql
# CREATE OR REPLACE VIEW TotalRevenueView AS
# SELECT
#     SUM(monthly_revenue) AS total_revenue,
#     SUM(CASE WHEN client_online = TRUE THEN monthly_revenue ELSE 0 END) AS total_online_revenue,
#     SUM(CASE WHEN client_online = FALSE THEN monthly_revenue ELSE 0 END) AS total_offline_revenue
# FROM FNOData
# WHERE date BETWEEN :start_date AND :end_date;
# ```
# 
# **Usage:**
# ```sql
# SELECT * FROM TotalRevenueView WHERE :start_date = '2023-01-01' AND :end_date = '2023-12-31';
# ```
# 
# ---
# 
# #### **3. `CustomerGrowthView`**
# Provides customer growth over time (new and total customers per day).
# 
# ```sql
# CREATE OR REPLACE VIEW CustomerGrowthView AS
# SELECT
#     date,
#     COUNT(DISTINCT client_name) AS new_customers,
#     SUM(COUNT(DISTINCT client_name)) OVER (ORDER BY date) AS total_customers
# FROM FNOData
# WHERE date BETWEEN :start_date AND :end_date
# GROUP BY date;
# ```
# 
# **Usage:**
# ```sql
# SELECT * FROM CustomerGrowthView WHERE :start_date = '2023-01-01' AND :end_date = '2023-12-31';
# ```
# 
# ---
# 
# #### **4. `RevenueGrowthView`**
# Provides revenue growth over time (total revenue per day).
# 
# ```sql
# CREATE OR REPLACE VIEW RevenueGrowthView AS
# SELECT
#     date,
#     SUM(monthly_revenue) AS total_revenue
# FROM FNOData
# WHERE date BETWEEN :start_date AND :end_date
# GROUP BY date;
# ```
# 
# **Usage:**
# ```sql
# SELECT * FROM RevenueGrowthView WHERE :start_date = '2023-01-01' AND :end_date = '2023-12-31';
# ```
# 
# ---
# 
# #### **5. `CustomerDistributionView`**
# Provides customer distribution by network.
# 
# ```sql
# CREATE OR REPLACE VIEW CustomerDistributionView AS
# SELECT
#     network,
#     COUNT(DISTINCT client_name) AS total_customers
# FROM FNOData
# WHERE date BETWEEN :start_date AND :end_date
# GROUP BY network;
# ```
# 
# **Usage:**
# ```sql
# SELECT * FROM CustomerDistributionView WHERE :start_date = '2023-01-01' AND :end_date = '2023-12-31';
# ```
# 
# ---
# 
# #### **6. `RevenueDistributionView`**
# Provides revenue distribution by network.
# 
# ```sql
# CREATE OR REPLACE VIEW RevenueDistributionView AS
# SELECT
#     network,
#     SUM(monthly_revenue) AS total_revenue
# FROM FNOData
# WHERE date BETWEEN :start_date AND :end_date
# GROUP BY network;
# ```
# 
# **Usage:**
# ```sql
# SELECT * FROM RevenueDistributionView WHERE :start_date = '2023-01-01' AND :end_date = '2023-12-31';
# ```
# 
# ---
# 
# #### **7. `NetworkMetricsView`**
# Provides customer and revenue metrics by network over time.
# 
# ```sql
# CREATE OR REPLACE VIEW NetworkMetricsView AS
# SELECT
#     date,
#     network,
#     COUNT(DISTINCT client_name) AS total_customers,
#     SUM(monthly_revenue) AS total_revenue
# FROM FNOData
# WHERE date BETWEEN :start_date AND :end_date
# GROUP BY date, network;
# ```
# 
# **Usage:**
# ```sql
# SELECT * FROM NetworkMetricsView WHERE :start_date = '2023-01-01' AND :end_date = '2023-12-31';
# ```
# 
# ---
# 
# ### **Dynamic Filtering with Start/End Dates**
# 
# To filter the views by **start and end dates**, you can pass the dates as parameters when querying the views. For example:
# 
# ```sql
# SELECT * FROM CustomerGrowthView
# WHERE date BETWEEN '2023-01-01' AND '2023-12-31';
# ```
# 
# ---
# 
# ### **Summary**
# 
# The updated `FNOData` table now includes all the required columns, and the views provide dynamic aggregations for the specified metrics. These views can be filtered by **start and end dates** to generate the required visualizations.
# 
# Let me know if you need further assistance!

# %% [markdown]
# 


