from prefect.deployments import Deployment
from web_to_gcs import etl_parent_flow
from gcs_to_bq import etl_gcs_to_bq
from prefect.infrastructure.docker import DockerContainer
from prefect.server.schemas.schedules import CronSchedule

docker_block = DockerContainer.load("zoom")

docker_dep_gcs = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name="docker-flow-web2gcs",
    infrastructure=docker_block,
    schedule=(CronSchedule(cron="0 23 * * 6")),
)

docker_dep_bq = Deployment.build_from_flow(
    flow=etl_gcs_to_bq,
    name="docker-flow-gcs2bq",
    infrastructure=docker_block,
    schedule=(CronSchedule(cron="30 23 * * 6")),
)

if __name__ == "__main__":
    docker_dep_gcs.apply()
    docker_dep_bq.apply()