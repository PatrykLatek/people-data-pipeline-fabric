import random
from datetime import datetime, timedelta
import csv

first_names = [
    "Patryk",
    "Kuba",
    "Piotr",
    "Pawel",
    "Michal",
    "Tomasz",
    "Kamil",
    "Mateusz",
    "Adam",
    "Lukasz",
    "Marek",
    "Karol",
    "Damian",
    "Marcin",
    "Bartosz",
    "Anna",
    "Katarzyna",
    "Agnieszka",
    "Magdalena",
    "Malgorzata",
    "Joanna",
    "Natalia",
    "Aleksandra",
    "Monika",
    "Zuzanna",
    "James",
    "Oliver",
    "George",
    "Harry",
    "Jack",
    "Charlie",
    "Thomas",
    "William",
    "Daniel",
    "Matthew",
    "David",
    "Joseph",
    "Samuel",
    "Henry",
    "Edward",
    "Emily",
    "Olivia",
    "Sophie",
    "Jessica",
    "Charlotte",
    "Amelia",
    "Grace",
    "Lucy",
    "Chloe",
    "Alice"
    ]

last_names = [
    "Kowalski",
    "Nowak",
    "Wisniewski",
    "Wojcik",
    "Kowalczyk",
    "Kaminski",
    "Lewandowski",
    "Zielinski",
    "Szymanski",
    "Wozniak",
    "Dabrowski",
    "Kozlowski",
    "Jankowski",
    "Mazur",
    "Kwiatkowski",
    "Krawczyk",
    "Piotrowski",
    "Grabowski",
    "Pawlowski",
    "Michalski",
    "Adamczyk",
    "Dudek",
    "Zajac",
    "Wieczorek",
    "Krupa",
    "Smith",
    "Johnson",
    "Williams",
    "Brown",
    "Jones",
    "Taylor",
    "Wilson",
    "Davies",
    "Evans",
    "Thomas",
    "Roberts",
    "Walker",
    "Wright",
    "Thompson",
    "White",
    "Hughes",
    "Edwards",
    "Green",
    "Hall",
    "Lewis",
    "Clark",
    "Baker",
    "Turner",
    "Parker",
    "Cooper"
    ]

department_ids = [10,20,30,40,50]
locations = [
    ("Warsaw", "Poland"),
    ("London", "UK"),
    ("Paris", "France"),
    ("Berlin", "Germany"),
    ("Madrid", "Spain"),
    ("Rome", "Italy"),
    ("Lisbon", "Portugal"),
    ("Prague", "Czechia"),
    ("Tokyo", "Japan"),
    ("Seoul", "South Korea"),
    ("Beijing","China"),
    ("Mumbai","India"),
    ("Dubai","UAE"),
    ("Sydney","Australia"),
    ("Toronto","Canada"),
    ("New York","USA"),
    ("Mexico City","Mexico"),
    ("Sao Paulo","Brazil"),
    ("Cairo","Egypt"),
    ("Cape Town","South Africa")
]
start_date = datetime(2019, 1, 1)
current_date = datetime.today()
employment_types = ["FULL_TIME","PART_TIME","CONTRACTOR"]
statuses = ["ACTIVE","INACTIVE"]

employees = []

for i in range(1000):
    employee_id = 1001 + i
    first_name = random.choice(first_names)
    last_name = random.choice(last_names)
    email = first_name.lower() + "." + last_name.lower() + str(employee_id) + "@company.com"
    department_id = random.choice(department_ids)
    city,country = random.choice(locations)
    random_days = random.randint(0,2500)
    hire_date = start_date + timedelta(days=random_days)
    employment_type = random.choice(employment_types)
    status = random.choice(statuses)
    salary = random.randint(6000,22000)
    difference = current_date - hire_date
    update_days = random.randint(0, difference.days)
    updated_at = hire_date + timedelta(days=update_days)
    employee = {"employee_id": employee_id,
                "first_name": first_name,
                "last_name": last_name,
                "email": email,
                "department_id": department_id,
                "hire_date": hire_date,
                "employment_type": employment_type,
                "country": country,
                "city": city,
                "status": status,
                "salary": salary,
                "updated_at":updated_at
                }

    

    if random.random() < 0.05:
        employee["salary"] = -500

    if random.random() < 0.03:
        employee["country"] = ""

    if random.random() < 0.04:
        employee["status"] = "UNKNOWN"

    if employees and random.random() < 0.02:
        employee["employee_id"] = random.choice(employees)["employee_id"]

    if random.random() < 0.03:
        employee["email"] = ""

    if random.random() < 0.02:
        employee["hire_date"] = "2026-99-99"

    if random.random() < 0.01:
        employee["hire_date"] = ""

    employees.append(employee)



fieldnames = [
    "employee_id",
    "first_name",
    "last_name",
    "email",
    "department_id",
    "hire_date",
    "employment_type",
    "country",
    "city",
    "status",
    "salary",
    "updated_at"
]


output_path = "employees.csv"

with open(output_path,"w",newline="") as plik:
    writer = csv.DictWriter(
        plik,
        fieldnames=fieldnames
        )
    writer.writeheader()
    writer.writerows(employees)



print("Generated records:", len(employees))
print("Source file created:", output_path)




