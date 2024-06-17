import pandas as pd
import numpy as np
from faker import Faker
import random

fake = Faker()

data = {
    'id': range(1, 11),
    'name': [fake.name() for _ in range(10)],
    'email': [fake.email() for _ in range(10)],
    'age': [random.randint(18, 70) for _ in range(10)],
    'join_date': [fake.date_between(start_date='-5y', end_date='today') for _ in range(10)]
}

df = pd.DataFrame(data)

print(df)

csv_path="/home/davisgriffith/Analytics/Python/pandas-example/randomData.csv"

try:
    df.to_csv(csv_path, index=False)
    print(f"Data saved to {csv_path}")
except Exception as e:
    print(e.with_traceback)
    
