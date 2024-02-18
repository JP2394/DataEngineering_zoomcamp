import dlt
import duckdb
''''
Question 1: What is the sum of the outputs of the generator for limit = 5?
Answer: 8.382332347441762
'''

def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1

# Example usage:
limit = 14
generator = square_root_generator(limit)
list_values  = []
for i,value in enumerate(generator):
    print(f'index: {i} value:{value}')

''''    
Question 2: What is the 13th number yielded by the generator?
Answer: 3.605551275463989
'''

def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1

# Example usage:
limit = 14
generator = square_root_generator(limit)

for i,value in enumerate(generator):
    print(f'index: {i} value:{value}')

'''
Question 3: Append the 2 generators. After correctly appending the data
calculate the sum of all ages of people.
Answer: 353
'''



def people_1():
    for i in range(1, 6):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}

for person in people_1():
    print(person)

def people_2():
    for i in range(3, 9):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}

for person in people_2():
    print(person)

# define the connection to load to.
# We now use duckdb, but you can switch to Bigquery later
generators_pipeline = dlt.pipeline(destination='duckdb', dataset_name='generators')

# we can load any generator to a table at the pipeline destnation as follows:
info = generators_pipeline.run(people_1(),
                    table_name="http_download",
                    write_disposition="replace")

# # we can load the next generator to the same or to a different table.
info = generators_pipeline.run(people_2(),
                    table_name="http_download",
                    write_disposition="append")

# the outcome metadata is returned by the load and we can inspect it by printing it.
print(info)

conn = duckdb.connect(f"{generators_pipeline.pipeline_name}.duckdb")

# let's see the tables
conn.sql(f"SET search_path = '{generators_pipeline.dataset_name}'")
# print('Loaded tables: ')
# display(conn.sql("show tables"))

# and the data

print("\n\n\n http_download table below:")

rides = conn.sql("SELECT sum(age) FROM http_download").df()
display(rides)


'''
Question 4: Merge the 2 generators using the ID column. Calculate the sum of ages
 of all the people loaded as described above.
 Answer: 266
'''

def people_1():
    for i in range(1, 6):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}

for person in people_1():
    print(person)

def people_2():
    for i in range(3, 9):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}

for person in people_2():
    print(person)

# define the connection to load to.
# We now use duckdb, but you can switch to Bigquery later
generators_pipeline = dlt.pipeline(destination='duckdb', dataset_name='generators')

# we can load any generator to a table at the pipeline destnation as follows:
info = generators_pipeline.run(people_1(),
                    table_name="http_download",
                    write_disposition="replace")

# # we can load the next generator to the same or to a different table.
info = generators_pipeline.run(people_2(),
                    table_name="http_download",
                    write_disposition="append")

info =  generators_pipeline.run(people_2(),
                    table_name="http_download",
                    write_disposition="merge",
                    primary_key="ID")

# the outcome metadata is returned by the load and we can inspect it by printing it.
print(info)

conn = duckdb.connect(f"{generators_pipeline.pipeline_name}.duckdb")

# let's see the tables
conn.sql(f"SET search_path = '{generators_pipeline.dataset_name}'")
# print('Loaded tables: ')
# display(conn.sql("show tables"))

# and the data

print("\n\n\n http_download table below:")

rides = conn.sql("SELECT SUM(age) FROM http_download").df()
display(rides)
