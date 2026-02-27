import os
import csv
import random
from datetime import datetime, timedelta

# -----------------------------
# Data setup
# -----------------------------
customer_ids = list(range(1, 21))
store_ids = list(range(121, 124))
product_data = {
    "quaker oats": 212,
    "sugar": 50,
    "maida": 20,
    "besan": 52,
    "refined oil": 110,
    "clinic plus": 1.5,
    "dantkanti": 100,
    "nutrella": 40
}
sales_persons = {
    121: [1, 2, 3],
    122: [4, 5, 6],
    123: [7, 8, 9]
}

# -----------------------------
# File location
# -----------------------------
file_location = "C:\\Users\\nikita\\Documents\\data_engineering\\spark_data"
os.makedirs(file_location, exist_ok=True)

# -----------------------------
# Input date
# -----------------------------
input_date_str = input("Enter the date for which you want to generate (YYYY-MM-DD): ")
input_date = datetime.strptime(input_date_str, "%Y-%m-%d")

# -----------------------------
# CSV file path
# -----------------------------
csv_file_path = os.path.join(file_location, f"sales_data_{input_date_str}.csv")

# -----------------------------
# Generate CSV
# -----------------------------
rows_to_generate = 400_000
progress_interval = 50_000  # print progress every 50k rows

with open(csv_file_path, "w", newline="") as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow([
        "customer_id", "store_id", "product_name", "sales_date",
        "sales_person_id", "price", "quantity", "total_cost"
    ])

    for i in range(1, rows_to_generate + 1):
        customer_id = random.choice(customer_ids)
        store_id = random.choice(store_ids)
        product_name = random.choice(list(product_data.keys()))
        sales_date = input_date  # can also randomize within a month if needed
        sales_person_id = random.choice(sales_persons[store_id])
        quantity = random.randint(1, 10)
        price = product_data[product_name]
        total_cost = round(price * quantity, 2)

        csvwriter.writerow([
            customer_id, store_id, product_name, sales_date.strftime("%Y-%m-%d"),
            sales_person_id, price, quantity, total_cost
        ])

        if i % progress_interval == 0:
            print(f"{i} rows generated...")

print("CSV file generated successfully:", csv_file_path)
