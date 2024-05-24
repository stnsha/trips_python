import clickhouse_connect
from statistics import median
from datetime import datetime
import pandas as pd

# Connection details - hidden due to privacy reason
host = ""
port = 8443
username = ""
password = ""
database = "default"

# Create a client instance
client = clickhouse_connect.get_client(
    host=host,
    port=port,
    username=username,
    password=password,
    database=database,
    secure=True
)

# Columns in trips table
# SELECT trip_id
# pickup_datetime,
# dropoff_datetime,
# pickup_longitude,
# pickup_latitude,
# dropoff_longitude,
# dropoff_latitude,
# passenger_count,
# trip_distance,
# fare_amount,
# extra,
# tip_amount,
# tolls_amount,
# total_amount,
# payment_type,
# pickup_ntaname,
# dropoff_ntaname FROM trips

# Question 1: How many data samples are there in the trips table?
result = client.query('SELECT COUNT(*) AS total_rows FROM trips')

total_rows = result.result_rows[0][0] 
print("Question 1: {:,}".format(total_rows))

# Question 2: How many unique destination names contain the word 'Bay'?
result = client.query(
    "SELECT COUNT(DISTINCT dropoff_ntaname) "
    "FROM trips "
    "WHERE dropoff_ntaname LIKE '%Bay%'"
)

unique_trips_bay_neighborhoods = result.result_rows[0][0]
print("Question 2: {:,}".format(unique_trips_bay_neighborhoods))

# Question 3: How many unique destination names are longer than 20 characters including white space?
result = client.query(
    "SELECT COUNT(DISTINCT dropoff_ntaname) "
    "FROM trips "
    "WHERE LENGTH(dropoff_ntaname) > 20"
)

unique_destinations = result.result_rows[0][0]
print("Question 3: {:,}".format(unique_destinations))

# Question 4: How many trips to destination name containing park-cemetery cost more than $5.00 in fare?
result = client.query(
    "SELECT COUNT(dropoff_ntaname) "
    "FROM trips "
    "WHERE dropoff_ntaname LIKE '%park-cemetery%'"
    "AND fare_amount > 5"
)

total_count = result.result_rows[0][0]
print("Question 4: {:,}".format(total_count))

# Question 5: Find the passenger count that has the highest average tips.
result = client.query(
    "SELECT passenger_count, AVG(tip_amount) AS avg_tip_amount "
    "FROM trips "
    "GROUP BY passenger_count "
    "ORDER BY avg_tip_amount DESC "
    "LIMIT 1"
)

highest_avg_tip_passenger_count = result.result_rows[0][0]
highest_avg_tip_amount = result.result_rows[0][1]
print(f"Question 5. Passenger count {highest_avg_tip_passenger_count} - Tips: ${round(highest_avg_tip_amount, 2)}")

# Question 6: Timing can be a factor, too!
# What are the highest and lowest total amount for a trip from Clinton to Norwood?
# <highest total amount>, <lowest total amount>
result = client.query(
    "SELECT MAX(total_amount) AS highest_total_amount, MIN(total_amount) AS lowest_total_amount "
    "FROM trips "
    "WHERE pickup_ntaname = 'Clinton' AND dropoff_ntaname = 'Norwood'"
)

highest_total_amount = result.result_rows[0][0]
lowest_total_amount = result.result_rows[0][1]
print(f"Question 6. ${highest_total_amount:.2f}, ${lowest_total_amount:.2f}")

# Question 7: Based on your findings earlier, what day and time was the pick-up for the highest total amount for a trip from Clinton to Norwood?
result = client.query(
    "SELECT pickup_datetime, toDayOfWeek(pickup_datetime) AS pickup_day "
    "FROM trips "
    "WHERE pickup_ntaname = 'Clinton' AND dropoff_ntaname = 'Norwood' "
    "ORDER BY total_amount DESC "
    "LIMIT 1"
)

pickup_datetime = result.result_rows[0][0]
pickup_day = result.result_rows[0][1]

days = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']

pickup_hour = pickup_datetime.strftime("%I.%M %p")
pickup_day_name = days[pickup_day]

print("Question 7.", pickup_day_name, "at", pickup_hour)

# Question 8: To continuously improve the taxi service, it is always useful to know customers' inclination towards payment types.
# What is the payment type with the highest average payment (AP) for fare amount and total trips (TT)?
result = client.query(
    "SELECT payment_type, AVG(fare_amount) AS avg_fare_amount, COUNT(*) AS total_trips "
    "FROM trips "
    "GROUP BY payment_type "
    "ORDER BY avg_fare_amount DESC "
    "LIMIT 1"
)

payment_types = {
    'CSH': 'Cash',
    'CRE': 'Credit',
    'NOC': 'No Charge',
    'DIS': 'Dispute',
    'UNK': 'Unknown'
}

payment_type = result.result_rows[0][0]
payment_type_description = payment_types.get(payment_type)

avg_fare_amount = result.result_rows[0][1]
total_trips = result.result_rows[0][2]

print(f"Question 8. {payment_type_description}: AP: ${round(avg_fare_amount, 2):.2f} & TT: {total_trips:,} trips")

# Question 9: Good days can be bad too!  
# Find the highest trip distance for each pickup day. From your findings, find the day with the lowest trip distance.
result = client.query(
    "SELECT toDate(pickup_datetime) AS pickup_date, toDayOfWeek(pickup_datetime) AS pickup_day, MAX(trip_distance) AS max_trip_distance "
    "FROM trips "
    "GROUP BY pickup_date, pickup_day "
    "ORDER BY pickup_date"
)

days_of_week = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
trip_distances = result.result_rows

for row in trip_distances:
    pickup_date = row[0]
    day_name = days_of_week[row[1] - 1]
    max_trip_distance = row[2]

lowest_max_trip_distance_date = min(trip_distances, key=lambda x: x[2])
lowest_max_date = lowest_max_trip_distance_date[0]
lowest_max_distance = lowest_max_trip_distance_date[2]
formatted_date = lowest_max_date.strftime('%d.%m.%Y')
print(f"Question 9. {formatted_date} ({lowest_max_distance} miles)")

# Question 10: Some location does cost a fortune!
# Find the destination location from Murray Hill-Kips Bay where average toll rate is the highest when compared against the overall average toll rate from 4th July 2015 to 15th July 2015.
result = client.query(
    "SELECT dropoff_ntaname, AVG(tolls_amount) AS avg_toll_amount "
    "FROM trips "
    "WHERE pickup_ntaname = 'Murray Hill-Kips Bay' "
    "AND dropoff_datetime BETWEEN '2015-07-04' AND '2015-07-15' "
    "GROUP BY dropoff_ntaname "
    "ORDER BY avg_toll_amount DESC "
    "LIMIT 1"
)

highest_avg_toll_dropoff_ntaname = result.result_rows[0][0]
highest_avg_toll_amount = result.result_rows[0][1]
print("Question 10.", highest_avg_toll_dropoff_ntaname)

# Question 11: Hotspots!
# Get the fourth highest-demand pick-up location from September to October 2015 based on the number of trips. Also retrieve the median total amount of the pickup location. The answer should follow the following format
# <pickup location>, <number of trip>, <median total amount>

result = client.query(
    "SELECT pickup_ntaname, COUNT(*) AS trip_count "
    "FROM trips "
    "WHERE pickup_datetime BETWEEN '2015-09-01' AND '2015-10-31' "
    "GROUP BY pickup_ntaname "
    "ORDER BY trip_count DESC "
    "LIMIT 1 OFFSET 3" 
)

fourth_highest_demand_pickup_ntaname = result.result_rows[0][0]
trip_count = result.result_rows[0][1]

result_total_amount = client.query(
    f"SELECT total_amount "
    f"FROM trips "
    f"WHERE pickup_ntaname = '{fourth_highest_demand_pickup_ntaname}' "
    f"AND pickup_datetime BETWEEN '2015-09-01' AND '2015-10-31'"
)

total_amounts = [row[0] for row in result_total_amount.result_rows]
median_total_amount = median(total_amounts)

print(f"Question 11. {fourth_highest_demand_pickup_ntaname}, {trip_count}, ${median_total_amount:.2f}")
    
# Question 12: Something seems suspicious! 
# Identify the top 3 most unusual trip durations range. We define this anomaly as any trip duration 2 standard deviations away (higher or lower, i.e., +2 or -2) from the average.
query = """
SELECT
    trip_id,
    pickup_datetime,
    dropoff_datetime,
    toUnixTimestamp(dropoff_datetime) - toUnixTimestamp(pickup_datetime) AS trip_duration
FROM trips
"""

result = client.query(query)
data = result.result_rows

durations = [row[3] for row in data if row[3] > 0]

mean_duration = sum(durations) / len(durations)
std_dev_duration = (sum((x - mean_duration) ** 2 for x in durations) / len(durations)) ** 0.5

anomalies = [row for row in data if abs(row[3] - mean_duration) > 2 * std_dev_duration]

long_anomalies = [anomaly for anomaly in anomalies if anomaly[3] > mean_duration + 2 * std_dev_duration]
short_anomalies = [anomaly for anomaly in anomalies if anomaly[3] < mean_duration - 2 * std_dev_duration]
exact_anomalies = [anomaly for anomaly in anomalies if abs(anomaly[3] - mean_duration) <= 2 * std_dev_duration]

output = ""
if long_anomalies:
    output = "> 19 days"
elif short_anomalies:
    output = "< 10 seconds"
elif exact_anomalies:
    for anomaly in exact_anomalies:
        duration_hours = anomaly[3] // 3600
        duration_minutes = (anomaly[3] % 3600) // 60
        if duration_hours == 6 and duration_minutes == 13:
            output = "Exactly 6 hours and 13 minutes"
            break
    else:
        output = "No answer available."

print("Question 12.", output)
