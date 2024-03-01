from pyspark import SparkContext, SparkConf
import math

conf = SparkConf().setAppName("CloseContactsApplication").setMaster("local[*]")
sc = SparkContext.getOrCreate(conf=conf)

# adjust your path
people_large_rdd = sc.textFile("")
infected_small_rdd = sc.textFile("")
people_some_infected_large_rdd = sc.textFile("")

# helpers
def parse_line(line):
    parts = line.split(',')
    return (int(parts[0]), float(parts[1]), float(parts[2]), parts[3:])

def parse_line_with_infection(line):
    parts = line.split(',')
    # Assuming the infection status is the last column
    return (int(parts[0]), float(parts[1]), float(parts[2]), parts[3], parts[4]) 

def distance(p1, p2):
    return math.sqrt((p1[0] - p2[0]) ** 2 + (p1[1] - p2[1]) ** 2)

#Query 1
# Parse the datasets
people_parsed = people_large_rdd.map(parse_line)
infected_parsed = infected_small_rdd.map(parse_line)

# Cartesian join and filter for distance <= 6 units
close_contacts = people_parsed.cartesian(infected_parsed) \
    .filter(lambda x: x[0][0] != x[1][0] and distance((x[0][1], x[0][2]), (x[1][1], x[1][2])) <= 6) \
    .map(lambda x: (x[0][0], x[1][0]))

# Collect the result
close_contacts_list = close_contacts.collect()

# Display or save the result as needed
print(close_contacts_list[:10])  # Print the first 10 results for inspection


# Query 2
# Distinct IDs of people who were in close contact
close_contact_ids = close_contacts.map(lambda x: x[0]).distinct()

# Collect the result
close_contact_ids_list = close_contact_ids.collect()

# Display or save the result as needed
print(close_contact_ids_list[:10])  # Print the first 10 results for inspection


#Query 3
# Parse the dataset
people_some_infected_parsed = people_some_infected_large_rdd.map(parse_line_with_infection)

# Filter to get only infected individuals
infected_people = people_some_infected_parsed.filter(lambda x: x[4] == "yes")

# Perform a self cartesian join and filter
close_contacts_counts = infected_people.cartesian(people_some_infected_parsed) \
    .filter(lambda x: x[0][0] != x[1][0] and distance((x[0][1], x[0][2]), (x[1][1], x[1][2])) <= 6) \
    .map(lambda x: (x[0][0], 1)) \
    .reduceByKey(lambda a, b: a + b)

# Collect the result
close_contacts_counts_list = close_contacts_counts.collect()

# Display or save the result as needed
print(close_contacts_counts_list[:10])  # Print the first 10 results for inspection
