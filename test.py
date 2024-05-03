import time
import random
import pandas as pd
from ezstorage import Sqlite, Table
from dataclasses import dataclass

db = Sqlite('test.db')

@db.useTable("table_name")
class User(Table):
    age: int
    name: str
    test: str = "test"


def create_users(count: int):
    for i in range(count):
        name = random.choice(['John', 'Doe', 'Jane', 'Smith', 'Michael', 'Jordan', 'Lebron', 'James', 'Kobe', 'Bryant'])
        age = random.randint(1, 100)
        user = User(name=name, age=age)
        user.save()
    db.commit()

    # with db:
    #     for i in range(count):
    #         name = random.choice(['John', 'Doe', 'Jane', 'Smith', 'Michael', 'Jordan', 'Lebron', 'James', 'Kobe', 'Bryant'])
    #         age = random.randint(1, 100)
    #         user = User(name=name, age=age)
    #         user.save()

# Drop the table if it exists, fresh start
db.drop_table(User)

# Create the table
db.create_table(User)

# Create a user
user = User(name='John', age=25)
user.save()

# Create 1000 users
create_users(1000)

users = User.where()
print(len(users))
users = User.where(lambda: User.age > 20) 
print(len(users))

df = pd.DataFrame(users)
print(df.head())


