import os
import csv
import random
from datetime import datetime

# -----------------------------
# Data setup
# -----------------------------
customer_ids = list(range(1, 21))
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
# Mapping store_id -> list of sales_person_ids
sales_persons = {
    121: [1, 2, 3],
    122: [4, 5, 6],
    123: [7, 8, 9]
}

# -----------------------------
# File location setup
# -----------------------------
file_location = "C:\\Users\\Utkarsh\\Documents\\data_engineering\\spark_data"
os.makedirs(file_location, exist_ok=True)

# -----------------------------
# Get date input from user
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
with open(csv_file_path, "w", newline="") as csvfile:
    csvwriter = csv.writer(csvfile)

    # Header
    csvwriter.writerow(["customer_id", "product_name", "sales_date", "sales_person_id",
                        "price", "quantity", "total_cost", "payment_mode"])

    for _ in range(200):
        customer_id = random.choice(customer_ids)
        product_name = random.choice(list(product_data.keys()))
        sales_date = input_date

        # Pick a random sales person from any store
        store_id = random.choice(list(sales_persons.keys()))
        sales_person_id = random.choice(sales_persons[store_id])  # pick single ID, not list

        quantity = random.randint(1, 10)
        price = product_data[product_name]
        total_cost = round(price * quantity, 2)
        payment_mode = random.choice(["cash", "UPI"])

        # Write row
        csvwriter.writerow([
            customer_id,
            product_name,
            sales_date.strftime("%Y-%m-%d"),
            sales_person_id,
            price,
            quantity,
            total_cost,
            payment_mode
        ])

print("CSV file generated successfully:", csv_file_path)
