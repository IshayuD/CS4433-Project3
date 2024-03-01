import random
import os

num_people_large = 10000  # Total number of people for the PEOPLE-large dataset
num_infected_small = 100  # Number of infected people for the INFECTED-small dataset

# PEOPLE-large dataset
people_large = [
    (
        i,
        random.randint(1, 10000),
        random.randint(1, 10000),
        random.randint(18, 80)
    )
    for i in range(1, num_people_large + 1)
]

# Select random subset of people to be marked as infected
infected_ids = set(random.sample(range(1, num_people_large + 1), num_infected_small))

# Generate INFECTED-small  from PEOPLE-large dataset
infected_small = [person for person in people_large if person[0] in infected_ids]

# Add "INFECTED" to PEOPLE-large to create PEOPLE-SOME-INFECTED-large
people_some_infected_large = [
    person + ("yes" if person[0] in infected_ids else "no",)
    for person in people_large
]

# saving data to root
script_directory = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(script_directory, "people-large.txt"), "w") as f_large, \
     open(os.path.join(script_directory, "infected-small.txt"), "w") as f_infected, \
     open(os.path.join(script_directory, "some-infected.txt"), "w") as f_some_infected:
    for person in people_large:
        f_large.write(','.join(map(str, person)) + "\n")
    for person in infected_small:
        f_infected.write(','.join(map(str, person)) + "\n")
    for person in people_some_infected_large:
        f_some_infected.write(','.join(map(str, person)) + "\n")
