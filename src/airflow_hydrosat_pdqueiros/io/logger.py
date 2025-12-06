from dagster import get_dagster_logger
from hydrosat_pdqueiros.services.settings import CODE_VERSION, SERVICE_NAME

logger = get_dagster_logger()
logger.info(f'Started {SERVICE_NAME} v{CODE_VERSION}')
