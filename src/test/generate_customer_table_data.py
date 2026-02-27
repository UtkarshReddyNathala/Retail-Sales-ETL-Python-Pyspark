import random
from faker import Faker
from datetime import datetime

# Initialize Faker for generating Indian names
fake = Faker('en_IN')

# Number of customer records to generate
num_records = 25

# Output SQL file to store insert statements
output_file = "customer_inserts.sql"

# Possible addresses and pincodes
addresses = ["Delhi", "Mumbai", "Bangalore", "Hyderabad", "Chennai"]
pincodes = ["110001", "400001", "560001", "500001", "600001"]

# Open the file for writing SQL insert statements
with open(output_file, "w") as f:
    for _ in range(num_records):
        # Generate random customer details
        first_name = fake.first_name()
        last_name = fake.last_name()
        address = random.choice(addresses)
        pincode = random.choice(pincodes)
        phone_number = '91' + ''.join([str(random.randint(0, 9)) for _ in range(10)])
        joining_date = fake.date_between_dates(
            date_start=datetime(2020, 1, 1), date_end=datetime(2023, 8, 20)
        ).strftime('%Y-%m-%d')

        # Prepare SQL insert statement
        sql_insert = (
            f"INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) "
            f"VALUES ('{first_name}', '{last_name}', '{address}', '{pincode}', '{phone_number}', '{joining_date}');\n"
        )

        # Write the statement to file
        f.write(sql_insert)

# Print summary
print(f"{num_records} customer insert statements generated in '{output_file}'.")
