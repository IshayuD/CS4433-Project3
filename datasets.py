from pyspark import SparkContext

# Create Spark context
sc = SparkContext("local", "PeopleLargeApp")

# Generate random 2D points with attributes to reach around 100MB
num_points_large = 250000  # Approximately 100MB
people_large_data = sc.parallelize(range(num_points_large)).map(lambda x: generate_random_point())
people_large_data.saveAsTextFile("hdfs://path/to/PEOPLE-large")

# Extract the infected IDs
infected_ids = people_large_data.takeSample(False, int(num_points_large * 0.1), seed=42)
infected_ids = [point[0] for point in infected_ids]

# Create INFECTED-small RDD with the same schema
infected_small_data = sc.parallelize(infected_ids).map(lambda id: generate_infected_point())
infected_small_data.saveAsTextFile("hdfs://path/to/INFECTED-small")

# Add infection status to PEOPLE-large based on INFECTED-small
people_some_infected_large_data = people_large_data.map(lambda point: add_infection_status(point, infected_ids))
people_some_infected_large_data.saveAsTextFile("hdfs://path/to/PEOPLE-SOME-INFECTED-large")

sc.stop()
