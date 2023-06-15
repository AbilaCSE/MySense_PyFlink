### How to run the Tasks
Docker build will add the source files into the image and will be available for execution
```
docker build . -t pyflink:mysense
```

There are 2 source files have been created to perform the task A(TaskA.py) and task B(TaskB.py), running with python will generate the output files in the output folder within the container.
However, inorder to see the output files directly on the host system, host volumns should be mounted while running the container as following

## Run - Task A
```
docker run  -v /path/to/mount/volumn/data:/opt/heart_rate_flink/data pyflink:mysense  /etc/poetry/bin/poetry run python /opt/heart_rate_flink/src/TaskA.py
```
Successful execution will produce the output files under your mounted data volumn
/path/to/mount/volumn/data/output/taskA_result/

## Run - Task B
```
docker run  -v /path/to/mount/volumn/data/data:/opt/heart_rate_flink/data  pyflink:mysense  /etc/poetry/bin/poetry run python /opt/heart_rate_flink/src/TaskB.py
```
Successful execution will produce the output files under your mounted data volumn
/path/to/mount/volumn/data/output/taskB_result/