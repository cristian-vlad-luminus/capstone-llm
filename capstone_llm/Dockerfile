FROM public.ecr.aws/dataminded/spark-k8s-glue:v3.5.1-hadoop-3.3.6-v1

USER 0
ENV PYSPARK_PYTHON python3
WORKDIR /opt/spark/work-dir

RUN python3 -m pip install --upgrade pip

# Copy the current directory contents into the container at /app
# COPY /src/capstonellm/tasks/clean.py /opt/spark/work-dir
COPY requirements.txt requirements.txt

# Install any needed dependencies specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . . 

RUN pip install .

CMD ["python3", "-m capstonellm.tasks.clean -e local -t dbt"]
