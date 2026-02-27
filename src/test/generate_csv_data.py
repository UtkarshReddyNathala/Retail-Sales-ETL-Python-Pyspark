import csv
import os
import random
from datetime import datetime, timedelta

# ------------------------------
# Configuration for CSV generation
# ------------------------------

# Customer IDs
customer_ids = list(range(1, 21))

# Store IDs
store_ids = list(range(121, 124))

# Product data: product_name -> price
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

# Sales persons per store
sales_persons = {
    121: [1, 2, 3],
    122: [4, 5, 6],
    123: [7, 8, 9]
}

# Date range for generating sales data
start_date = datetime(2023, 3, 3)
end_date = datetime(2023, 8, 20)

# Output CSV file location
file_location = "C:\\Users\\nikita\\Documents\\data_engineering\\spark_data"
csv_file_path = os.path.join(file_location, "sales_data.csv")

# Ensure output folder exists
if not os.path.exists(file_location):
    os.makedirs(file_location)

# ------------------------------
# Generate CSV file
# ------------------------------
with open(csv_file_path, "w", newline="") as csvfile:
    csvwriter = csv.writer(csvfile)

    # Write header row
    csvwriter.writerow([
        "customer_id",
        "store_id",
        "product_name",
        "sales_date",
        "sales_person_id",
        "price",
        "quantity",
        "total_cost"
    ])

    # Generate 500 sales records
    for _ in range(500):
        customer_id = random.choice(customer_ids)
        store_id = random.choice(store_ids)
        product_name = random.choice(list(product_data.keys()))
        sales_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
        sales_person_id = random.choice(sales_persons[store_id])
        quantity = random.randint(1, 10)
        price = product_data[product_name]
        total_cost = price * quantity

        # Write one row to CSV
        csvwriter.writerow([
            customer_id,
            store_id,
            product_name,
            sales_date.strftime("%Y-%m-%d"),
            sales_person_id,
            price,
            quantity,
            total_cost
        ])

# ------------------------------
# Print summary
# ------------------------------
print(f"CSV file generated successfully at: {csv_file_path}")
