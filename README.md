# airflow-hydrosat-pdqueiros

# Setup

## Base requirements

You need to have these installed on your machine:
- Install [Docker Desktop](https://www.docker.com/get-started/) if running this repo on WSL

## Deployment

To deploy all the necessary containers run:
```bash
# we are using a custom Airflow image
docker compose -f docker-compose.yaml build
docker compose -f docker-compose-airflow.yaml build
# deploys airflow
docker compose -f docker-compose-airflow.yaml up -d
# deploys minio for S3 ssimulation
docker compose -f docker-compose-storage.yaml up -d
# deploys multiple monitoring tools, including otel-collector (collected by Airflow), grafana (for dashboarrds) and mailhog (for failure callbacks)
docker compose -f docker-compose-monitoring.yaml up -d
```


Assuming everything was deployed correctly, you should now have access to all the necessary services and you can check their respective dashboards at:

- [Minio](http://localhost:9001) (credentials: minio/minio123)
- [Grafana](http://localhost:3000/login) (credentials: admin/admin)
- [Airflow](http://localhost:8080/) (credentials: airflow/airflow)
- [Prometheus](http://localhost:9090/)



When you access the [Minio](http://localhost:9001) dashboard, you should see 1 buckets `hydrosat-pdqueiros` where you will load your test data, which you can generate with `airflow_hydrosat_pdqueiros/tests/create_sample_data.py`.

### Minio connection setup

Now, go to [Airflow](http://localhost:8080/) and you should see no DAGs due to :

```bash
Traceback (most recent call last):
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/definitions/variable.py", line 53, in get
    return _get_variable(key, deserialize_json=deserialize_json)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/execution_time/context.py", line 265, in _get_variable
    raise AirflowRuntimeError(
airflow.sdk.exceptions.AirflowRuntimeError: VARIABLE_NOT_FOUND: {'message': 'Variable MINIO_CONNECTION not found'}
```

![airflow-dags-error](./images/airflow-dags-error.png)



**This is expected since we didn't yet create the necessary connection in Airflow.**

So go to the Airflow UI, and create the connection with these details:

- in the `Connection ID` field add `minio_connection` (that's what we defined in the `.env` but you could change it)
- Connection type: `Amazon Web Services`
- AWS Access Key ID: `BEDa33cuSs9aahxEWzG` (also defined in the `.env`)
- AWS Secret Access Key: `zsKNU39z7TlmyqiqwdxL6ZtCk9TpvsH3AMJX9qDl` (also defined in the .`env`)
- In the Extra Fields JSON add this:
```json
{
    "endpoint_url": "http://storage-minio:9000"
}
```
![minio-connection](./images/minio-connection.png)

Save the connection.

Now, while I've used load_dotenv to load our environmental variables (by mounting the `.env` file), in a production environment you are better off defining variables through the UI and then using `from airflow.sdk import Variable`. You can see the `MINIO_CONNECTION` example in the code and in the image below:

So go to the Airflow UI again and create this Variable:
- Key: `MINIO_CONNECTION`
- Value: `minio_connection` (this is the connection ID of the connection you created above)

![minio-connection-variable](./images/minio-connection-variable.png)

Save the variable and wait 10-30 seconds. Once Airflow refreshes the DAGs, you should be able to see the workflow DAGs.


# Description

Note that the task below reflects the project in this [repo](https://github.com/PedroMTQ/hydrosat_pdqueiros), which cover all aspects mentioned below. This repo however focuses solely on showcasing Dagster to Airflow migration while focusing less on cloud infra deployment.

## Requirements

- Dagster with k8s for scheduling and data processing
- Daily data that is split into partitions.
- Each partition is dependent on the preceeding day.
- Files should be read and written to S3 bucket (e.g., AWS)

## Task description

Data shall have daily partitions, where each partition depends on the partition of the preceding day. Further, it shall read asset inputs from and write asset outputs to a configurable S3 bucket (or an equivalent of the latter).

The asset logic should account for the following aspects:

- Asset inputs:
    - A square/rectangular bounding box, which acts as the processing extent for any geospatial operation
    - Multiple “fields”, represented as polygons, which intersect with the bounding box and have different planting dates

- Processing:
    - The asset should download or simulate any data of choice and process it within the extent of the bounding box. It shall provide some output values obtained on the field extent for each of the fields, starting at the field’s planting date.

- Asset output:
    - The asset output should be one or multiple files containing the output values per field per daily partition

- Complication:
    - Assume that some field data arrives late, e.g. because they were entered late inthe system. This means that the asset’s processing status has reached timepointt, but the field should have been processed at timepoint t-2. How to handle this situation without reprocessing the entire bounding box?

# Workflow


## General workflow


![workflow](images/workflow.drawio.png)

You can check the DAGs in detail here `airflow_hydrosat_pdqueiros/dags/dags.py`

The sensor for the fields has a few dependencies, as per the requirements:

- bounding box needs to be processed (currently by box id)
- previous field data is processed
- field date falls within partition start date

The bounding box processing has no dependencies.

### Data archiving

Note that after being processed, all data is archived into `archived_input` folders.

### Data locks

Note that when a task for a specific file starts, the respective file is locked in S3, meaning that the S3 sensor won't pick up this task again unless the task that locked the file fails.
You can check how this works here `airflow_hydrosat_pdqueiros/src/airflow_hydrosat_pdqueiros/core/base_task.py`


## Data format

Data is in jsonl format, both fields and bounding boxes have the same type of data, we just process them internally in a different manner.
Bounding box:
```
{"box_id": "01976dbcbdb77dc4b9b61ba545503b77", "coordinates_x_min": 97, "coordinates_y_min": 28, "coordinates_x_max": 112, "coordinates_y_max": 42, "irrigation_array": [[1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1], [1, 0, 0, 1, 0, 0, 1, 0, 0, 0, 1, 0, 1, 0, 1], [1, 1, 1, 0, 1, 0, 0, 1, 0, 0, 1, 0, 1, 0, 0], [0, 1, 0, 1, 1, 0, 1, 1, 1, 0, 0, 0, 0, 1, 0], [1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 0, 0, 1, 1, 1], [1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 0, 0, 0, 1, 1], [1, 1, 1, 1, 0, 1, 1, 0, 1, 0, 0, 1, 0, 1, 1], [0, 1, 0, 0, 1, 0, 0, 1, 0, 1, 0, 1, 0, 1, 1], [0, 1, 1, 0, 0, 1, 1, 1, 1, 0, 1, 0, 0, 0, 1], [1, 1, 1, 0, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1], [1, 0, 1, 0, 1, 1, 1, 0, 0, 0, 1, 0, 1, 1, 1], [1, 1, 1, 1, 0, 0, 1, 1, 1, 0, 0, 1, 1, 1, 1], [1, 1, 0, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 0], [0, 1, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 1, 0]], "is_processed": false}
```

Fields:
```
{"box_id": "01976dbcbdba78e1ba120a45b75e45da", "coordinates_x_min": 10, "coordinates_y_min": 6, "coordinates_x_max": 16, "coordinates_y_max": 8, "irrigation_array": [[0.0, 0.0, 0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0, 0.0, 0.0]], "is_processed": false}
{"box_id": "01976dbcbdb77dc4b9b61ba545503b77", "coordinates_x_min": 7, "coordinates_y_min": 4, "coordinates_x_max": 9, "coordinates_y_max": 6, "irrigation_array": [[0.0, 0.0], [0.0, 0.0]], "is_processed": false}
```

After processing, the flag `is_processed` is set to True.

Paths are equivalent in S3 and locally (but in locally, we store in the `tmp` folder)

```
/boxes/input/bounding_box_01976dbcbdb77dc4b9b61ba545503b77.jsonl
/boxes/output/bounding_box_01976dbcbdb77dc4b9b61ba545503b77.jsonl
fields/input/01976dbcbdb77dc4b9b61ba545503b77/fields_2025-06-02.jsonl
fields/output/01976dbcbdb77dc4b9b61ba545503b77/fields_2025-06-02.jsonl
```

These data types are implemented as data classes `src/hydrosat_pdqueiros/services/core/documents/bounding_box_document.py` and `src/hydrosat_pdqueiros/services/core/documents/field_document.py`. 
**Since we are not dong any real data transformations, I assume that fields are rectangular (similar to bounding boxes)**

## Dependencies testing

For dependencies testing you can remove some of the boxes/fields data from s3 and delete any past runs in the dagster UI. You can then upload the data files one by one and see how the dependencies are tracked in the sensors.
You can check task dependencies here `airflow_hydrosat_pdqueiros/src/airflow_hydrosat_pdqueiros/core/get_tasks.py`

### Note on late data arrival

Regarding the complication describe above (i.e., adding fields data on different timepoints without reprocessing bounding boxes):
- Upload file to the correct S3 folder, e.g., fields/input/01976dbcbdb77dc4b9b61ba545503b77/fields_2025-06-02_THIS_IS_A_RANDOM_STRING.jsonl
- wait for sensor to check dependencies

Keep in mind that we don't do any assets aggregation since this would depend on downstream business logic.










