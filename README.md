# Couchbase Benchmark Application

This application is designed to benchmark the performance of Couchbase under concurrent read and write operations. It allows users to configure various parameters such as thread count, number of runs, and duration of each run.

## **Features**

- Performs write and read operations on Couchbase using multiple threads.
- Collects metrics such as total operations, errors, and average latencies.
- Retrieves and displays Couchbase cluster and bucket metrics.
- Allows customization through the `application.properties` file.

## **Prerequisites**

- Couchbase Server installed and running.

## **Setup Instructions**

### **1. Clone the Repository**

```bash
git clone https://github.com/DanielBashary/couchbase-benchmark.git
```

### **2. Configure Application Properties**

Edit the `application.properties` file located in the `src/main/resources` directory. 
Update the following properties with your Couchbase cluster details and desired benchmark 
configurations:
```properties
# Couchbase cluster configuration
couchbase.host=127.0.0.1          # Replace with your Couchbase host address
couchbase.username=Administrator  # Replace with your Couchbase username
couchbase.password=               # Replace with your Couchbase password
couchbase.bucket=json-store       # Replace with the bucket name you want to use

# Benchmark configuration
threadCount=100                   # Number of threads to use in the benchmark
threadPoolRuns=5                  # Number of times to repeat the benchmark
processSeconds=5                  # Duration of each benchmark run in seconds
sleepBetweenRuns=0                # Seconds to sleep between benchmark runs
```

### **3. Build the Application Using Gradle**

Use Gradle to build the project:

`gradle build`

### **4. Run the Application**

After a successful build, run the application