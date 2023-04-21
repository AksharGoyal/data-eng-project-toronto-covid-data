FROM prefecthq/prefect:2.10-python3.9

COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir

RUN mkdir -p /opt/prefect/flows/

COPY web_to_gcs.py /opt/prefect/flows
COPY gcs_to_bq.py /opt/prefect/flows
COPY etl_parent_flow-deployment.yaml /opt/prefect/flows
COPY etl_gcs_to_bq-deployment.yaml /opt/prefect/flows