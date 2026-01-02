import csv
from datetime import datetime, timedelta

start_date = datetime(2015, 1, 1)
end_date = datetime(2024, 12, 31)

with open('date_dim.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['date_key', 'full_date', 'year', 'quarter_number', 'month_name', 'month_number', 'day_name', 'day_of_month'])

    current_date = start_date
    while current_date <= end_date:
        date_key = int(current_date.strftime('%Y%m%d'))
        full_date = current_date.strftime('%Y-%m-%d')
        year = current_date.year
        quarter = (current_date.month - 1) // 3 + 1
        month = current_date.month
        month_name = current_date.strftime('%B')
        day = current_date.day
        day_name = current_date.strftime('%A')

        writer.writerow([date_key, str(full_date), year, quarter, month_name, month, day_name, day])
        
        current_date += timedelta(days=1)