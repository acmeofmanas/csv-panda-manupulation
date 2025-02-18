import pandas as pd
import random
import string
from datetime import datetime, timedelta

def generate_random_email(name):
    domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'example.com']
    return f"{name.lower().replace(' ', '.')}@{random.choice(domains)}"

def generate_random_date():
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2024, 12, 31)
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    return start_date + timedelta(days=random_number_of_days)

def generate_random_score():
    return round(random.uniform(0, 100), 2)

def generate_csv(num_rows=200, filename='200-random_data.csv'):
    # Lists for generating random names
    first_names = ['John', 'Jane', 'Michael', 'Emily', 'David', 'Sarah', 'James', 'Emma', 
                   'William', 'Olivia', 'Robert', 'Sophia', 'Charles', 'Ava', 'Joseph', 'Mia']
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 
                  'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson']
    
    # Generate data
    data = {
        'id': range(1, num_rows + 1),
        'user': [f"{random.choice(first_names)} {random.choice(last_names)}" for _ in range(num_rows)],
        'email': [],  # Will be generated based on usernames
        'last_login': [generate_random_date() for _ in range(num_rows)],
        'activity_score': [generate_random_score() for _ in range(num_rows)],
        'status': [random.choice(['Active', 'Inactive', 'Pending', 'Suspended']) for _ in range(num_rows)]
    }
    
    # Generate emails based on usernames
    for user in data['user']:
        data['email'].append(generate_random_email(user))
    
    # Create DataFrame and save to CSV
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)
    
    # Display preview of the generated data
    print(f"\nGenerated CSV file: {filename}")
    print("\nFirst few rows of the generated data:")
    print(df.head())
    print("\nDataset Info:")
    print(df.info())

# Generate the CSV file
if __name__ == "__main__":
    generate_csv(num_rows=100, filename='200-user_data.csv')
