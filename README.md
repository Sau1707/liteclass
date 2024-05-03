# Ezstorge

> [!CAUTION]
> Currently in development, not ready for use

A python ORM (Object-Relational Mapping) that allow to use a simple class syntax to interact with a database.

## Why Ezstorge?

I needed to use a simple ORM to interact with a database, but I found that most of the existing ORMs are either too complex for a simple use case, and the migration tools are not always easy to setup and use.

-   **Simple**:

    -   The type of the field is defined by the type hint
    -   Has a simple query syntax similar to [PonyORM](https://ponyorm.org/)

-   **Flexible**:

    -   Automatic table creation
    -   Automatic table update (add and remove fields)
    -   No need to use extra migration tools

-   **Beautiful**:
    -   Designed to be beautiful and easy to read.
    -   Fully typed, for better IDE support

> [!NOTE]
> Inspired by [PonyORM](https://ponyorm.org/) and [Sqlalchemy](https://www.sqlalchemy.org/)

## Example

```python
import pandas as pd
import ezstorge as ez

db = ez.Sqlite("example.db")

@db.useTable("cars")
class Car(ez.Table):
    model: str      # Required string field
    price: float    # Required float field
    miles: int = 0  # Default value if not provided

# Create, update, delete table
db.createTable(Car)
db.createTables()
db.updateTable(Car)
db.updateTables()
db.deleteTable(Car)
db.deleteTables()

car = Car(model="Tesla", price=100)
car.save()          # Save the car
db.commit()         # Commit the change

cars = Car.where() # Get all cars
print(cars)

# Use lambda to filter
cars = Car.where(lambda: Car.price > 20)
print(cars)

# Create a dataframe from the table
df = pd.DataFrame(users)
print(df)
```

## Types

-   [ ] int
-   [ ] float
-   [ ] str
-   [ ] bool
-   [ ] datetime
-   [ ] date
-   [ ] time
-   [ ] timedelta
-   [ ] list
-   [ ] dict
-   [ ] set
-   [ ] tuple
-   [ ] json
-   [ ] uuid

## Providers

-   [ ] Excel
-   [ ] Sqlite
-   [ ] Microsoft Access
-   [ ] MySQL
-   [ ] PostgreSQL
