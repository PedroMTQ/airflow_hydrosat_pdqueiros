# hydrosat-pdqueiros

Tools used
- Amazon's [S3](https://eu-central-1.console.aws.amazon.com/s3) and Amazon's [ECR](https://eu-central-1.console.aws.amazon.com/ecr)
- Dagster for orchestration
- K8s for pod deployment and auto-scaling of dagster as pods (one pod per asset)
- Terraform for infrastructure creation

# TLDR, i.e., minikube+terraform

## Setup
1. Install [K8s](https://kubernetes.io/docs/tasks/tools/install-kubectl-linux/)
2. Install [Terraform](https://developer.hashicorp.com/terraform/install)
3. Install [minikube](https://minikube.sigs.k8s.io/docs/start/?arch=%2Flinux%2Fx86-64%2Fstable%2Fbinary+download). We are running minikube since we are deploying a k8s cluster locally.
4. **Create .env file with these values:**

```
# credentials
AWS_DEFAULT_REGION=
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
S3_BUCKET=

#static variables
DATE_FORMAT=%Y-%m-%d
S3_DATE_REGEX=\d{4}-\d{2}-\d{2}
FIELDS_FOLDER_INPUT=fields/input
FIELDS_FOLDER_OUTPUT=fields/output
BOXES_FOLDER_INPUT=boxes/input
BOXES_FOLDER_OUTPUT=boxes/output
FIELDS_PATTERN=fields_\d{4}-\d{2}-\d{2}(.*)?\.jsonl$
BOXES_PATTERN=bounding_box_.*\.jsonl
START_DATE=2025-06-02
```
I've included a `.env-template` you can just rename to `.env` and add yhour AWS credentials.
**After** this is done you can deploy:
5. Login to ECR so you can pull the latest image:
```bash
# if you haven't set the AWS credentials in your .bashrc file, you need to export them with:
source env.sh
# and then just login:
aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws
```


## Deployment


```
minikube start
# then start terraforming...
terraform init
terraform plan
terraform apply
# in another console you can check the dashboard with:
minikube dashboard
```

**If you are checking the minikube dashboard, make sure you use the correct namespace, i.e., "hydrosat-pdqueiros"**

Generally it will take some time for terraform to finish since it waits until all deployments are done

You can already check the minikube dashboard, but later on to work with dagster you can do this to enable port forwarding:
```bash
export DAGSTER_WEBSERVER_POD_NAME=$(kubectl get pods --namespace hydrosat-pdqueiros -l "app.kubernetes.io/name=dagster,app.kubernetes.io/instance=dagster,component=dagster-webserver" -o jsonpath="{.items[0].metadata.name}")
kubectl --namespace hydrosat-pdqueiros port-forward $DAGSTER_WEBSERVER_POD_NAME 8080:80
```

and then go to `http://127.0.0.1:8080`

**Note that port forwarding needs to be running whenever you want to work with dagster**


## Destroy deployment

```bash
terraform destroy
```

# Description

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

![dagster_workflow](images/dagster_workflow.png)

```python
defs = Definitions(
    assets=[asset_bounding_box, asset_fields],
    jobs=[job_process_fields, job_process_bounding_boxes],
    sensors=[sensor_fields, sensor_bounding_boxes],
    resources={
        "s3_resource": s3_resource,
        "io_manager_fields": io_manager_fields,
        "io_manager_bounding_box": io_manager_bounding_box,
    },
)
```

![dagster_workflow](images/dagster.drawio.png)



The sensor for the fields has a few dependencies, as per the requirements:

- bounding box needs to be processed (currently by box id)
- previous field data is processed
- field date falls within partition start date

The bounding box processing has no dependencies.




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


### Note on late data arrival

Regarding the complication describe above (i.e., adding fields data on different timepoints without reprocessing bounding boxes):
- Upload file to the correct S3 folder, e.g., fields/input/01976dbcbdb77dc4b9b61ba545503b77/fields_2025-06-02_THIS_IS_A_RANDOM_STRING.jsonl
- wait for sensor to check dependencies

Check `sensors.py/fields_dependencies_are_available` in sensors.py for an overview of how this works.

Keep in mind that we don't do any assets aggregation since this would depend on downstream business logic.



# Local deployment

The section below is mostly for development purposes; the only infra requirement we have is postgres, for that *make sure the postgres credentials match the ones found in the `dagster.yaml` file*

## Initial setup

1. Setup .env file

```
AWS_DEFAULT_REGION=
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
S3_BUCKET=
```

1. Export environmental variables:

```bash
source env.sh
```


1. Deploy postgres:

```bash
docker compose -f docker-compose-infra.yaml up -d
```

If you can't bind to postgres, e.g., you get this error:

```bash
Error response from daemon: driver failed programming external connectivity on endpoint proma-postgres-1 (a484fad4f83094cb257ff159fde87c1c3c1cb6bf7e9ebf6fc84ecbfd99b003ca): Error starting userland proxy: listen tcp4 127.0.0.1:5432: bind: address already in use
```

You can run:

```bash
# assuming the port for postgres is 5432
sudo lsof -t -i:5432 | xargs sudo kill -9
```


2. Create S3 bucket if needed (same name as `S3_BUCKET`)

3. [Install UV](https://docs.astral.sh/uv/getting-started/installation/) (**recommended**) and activate your environment with:
*Keep in mind that the `activate.sh` command assumes you are using UV for enviorenment management, if you prefer use something else like venv, conda, mamba, etc*
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
source activate.sh
```

4. Create test data and upload it to S3:
```bash
python tests/create_sample_data.py
```

5. Launch dagster dev:

```bash
# check definitions:
dg list defs
# run:
uv run --active dagster dev
```

6. Launch dagster-webserver:
```bash
dagster-webserver
```



# Deployment with minikube+helm

This section was the second develoment step, i.e., putting together the infrastructure. I've kept things simple by using Dagster's default helm chart with only the essential changes so that we can run the public image of this codebase.

## Tools installation

1. Install [K8s](https://kubernetes.io/docs/tasks/tools/install-kubectl-linux/)
2. Install [Helm](https://helm.sh/docs/intro/install/)
3. Install [Terraform](https://developer.hashicorp.com/terraform/install)
4. Install [minikube](https://minikube.sigs.k8s.io/docs/start/?arch=%2Flinux%2Fx86-64%2Fstable%2Fbinary+download). We are running minikube since we are deploying a k8s cluster locally.


## Docker image and tools deployment

1. Authenticate to Amazon ECR (this is the public registry I've set). This step is only needed if you need to modify the image.

```bash
aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws
docker compose build
docker tag hydrosat-pdqueiros:latest public.ecr.aws/d8n7f1a1/hydrosat_pdqueiros:latest
docker push public.ecr.aws/d8n7f1a1/hydrosat_pdqueiros:latest
```

You should see an image here:
https://eu-central-1.console.aws.amazon.com/ecr/repositories/public/996091555539/hydrosat_pdqueiros?region=eu-central-1

2. Start minikube with:
```bash
# we need this insecure registry to loag the image from localhost
# see https://gist.github.com/trisberg/37c97b6cc53def9a3e38be6143786589
minikube start
# Check node status
kubectl get nodes
# you should get something like this: `minikube   Ready    control-plane   33s   v1.33.1`
# set kubectl alias 
alias kubectl="minikube kubectl --"
kubectl config use-context minikube
```

2. Start minikube dashboard
```bash
minikube dashboard
```


*The output of the image list should match with the service_image variable in `dagster-chart.yaml`* (see below)


3. Deploy the service and dagster with [Helm](https://docs.dagster.io/deployment/oss/deployment-options/kubernetes/deploying-to-kubernetes). I've already set the chart file, so you don't need to change anything.

List of changes:
```yaml
global:
  serviceAccountName: "hydrosat-pdqueiros"

dagster-user-deployments:
  deployments:
    - name: "hydrosat-pdqueiros"
      image:
        repository: "public.ecr.aws/d8n7f1a1/hydrosat_pdqueiros"
        tag: latest
      dagsterApiGrpcArgs:
        - "--python-file"
        - "src/hydrosat_pdqueiros/defs/definitions.py"
      envSecrets: 
        - name: hydrosat-pdqueiros-secret
```

Now run:



```

# if the namespace does not exist
kubectl create namespace hydrosat-pdqueiros
# create the secret with the necessary env vars (if it doesnt exist)
# make sure you always check the if you have the secret with 
kubectl describe secret hydrosat-pdqueiros-secret -n hydrosat-pdqueiros
# if you don't run the command below
kubectl create secret generic hydrosat-pdqueiros-secret --from-env-file=.env -n hydrosat-pdqueiros

# set minikube config 
kubectl config use-context minikube
# and check it
kubectl config view
kubectl config set-context minikube --namespace hydrosat-pdqueiros --cluster minikube --user=hydrosat-pdqueiros


# get dagster chart
helm repo add dagster https://dagster-io.github.io/helm
helm repo update
```

4. Add env variables as a K8s secret:
```bash
kubectl create secret generic hydrosat-pdqueiros-secret --from-env-file=.env -n hydrosat-pdqueiros
```




And deploy it:
```bash
helm upgrade --install dagster dagster/dagster -f dagster-chart.yaml
```

Check the dashboard and see if the pods are running
![k8s_dashboard](images/k8s.png)

You probably won't have any data in your bucket
![k8s_dashboard](images/k8s_logs_no_data.png)

So now just run the test sample creation with
```bash
source env.sh
source activate.sh
python tests/create_sample_data.py
```

You can then manually add the data to the bucket


After adding bounding boxes data:

![k8s_dashboard](images/k8s__logs_added_bounding_boxes.png)

You can then see the dashboard and find that it ran some jobs:
![k8s_dashboard](images/k8s_bounding_boxes_dashboarb.png)

And one of the jobs:
![k8s_dashboard](images/k8s_bounding_boxes_job.png)

And if you check s3 you will the output from the job:
![k8s_dashboard](images/s3_bounding_boxes_output.png)


Now let's try with fields data:
![k8s_dashboard](images/fields_data.png)

You can see the job has run
![k8s_dashboard](images/fields_process_job_tags.png)


Congratulations for making it to the end! If you want a simplified versionn go back to the [top](#tldr-ie-minikubeterraform) and have fun with your deployed service.


# Future TODO

Most of the business logic was implemented to be quite simple, i.e., to respect the exercise requirements, but not to make further assumptions on additional requirements; these should be enough for a POC, whereas further complexity would need to be added on a per-business logic requirements basis.

You can find below some points which I imagine would be the next logical steps for a more robust workflow:

- Convert FieldDocument to process diverse polygons and not just squares.
- Add FieldDocument processing, i.e., compare field data to bounding box data and any other relevant assets. You can check some dummy field processing in `src/hydrosat/services/core/documents/field_document.py`
- Add actual bounding box processing, but that would be more complex than required for this exercise, I imagine doing something like saving the bounding box data into PGIS and then making it queriable by the field coordinates. But again, that would add a lot more complexity
- Add proper run logging (postgres) or find a way to extract the correct run ids from dagster (see `has_event_record` in sensors.py)
- Improve on [late date arrival](#note-on-late-data-arrival)
- Configure dagster properly, e.g., setup credentials
- Pass secrets correctly, i.e., the "terraformic" way 
