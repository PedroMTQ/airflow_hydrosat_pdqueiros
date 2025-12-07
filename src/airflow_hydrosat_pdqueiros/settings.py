import importlib.metadata
import os

SERVICE_NAME = 'airflow_hydrosat_pdqueiros'
CODE_VERSION = importlib.metadata.version(SERVICE_NAME)

ROOT = os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))
DATA = os.path.join(ROOT, 'data')
TEMP = os.path.join(ROOT, 'tmp')
TESTS = os.path.join(ROOT, 'tests')

DEBUG = bool(int(os.getenv('DEBUG', '0')))

START_DATE = os.getenv('START_DATE', '2025-06-02')

# AWS
S3_BUCKET = os.getenv('S3_BUCKET')
S3_DATE_REGEX = os.getenv('S3_DATE_REGEX')
DATE_FORMAT = os.getenv('DATE_FORMAT')

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
MINIO_HOST = os.getenv('MINIO_HOST')
MINIO_PORT = os.getenv('MINIO_PORT')

FIELDS_FOLDER_INPUT = os.getenv('FIELDS_FOLDER_INPUT')
FIELDS_FOLDER_ARCHIVED_INPUT = os.getenv('FIELDS_FOLDER_ARCHIVED_INPUT')
FIELDS_FOLDER_OUTPUT = os.getenv('FIELDS_FOLDER_OUTPUT')

BOXES_FOLDER_INPUT = os.getenv('BOXES_FOLDER_INPUT')
BOXES_FOLDER_ARCHIVED_INPUT = os.getenv('BOXES_FOLDER_ARCHIVED_INPUT')
BOXES_FOLDER_OUTPUT = os.getenv('BOXES_FOLDER_OUTPUT')
FIELDS_PATTERN = os.getenv('FIELDS_PATTERN')
SENSOR__FIELD_PATTERN = os.getenv('SENSOR__FIELD_PATTERN')
BOXES_PATTERN = os.getenv('BOXES_PATTERN')


FIELDS_SENSOR_SLEEP_TIME = int(os.getenv('FIELDS_SENSOR_SLEEP_TIME', '10'))
BOXES_SENSOR_SLEEP_TIME = int(os.getenv('BOXES_SENSOR_SLEEP_TIME', '10'))

MATERIALIZATIONS_FETCHER_LIMIT = int(os.getenv('MATERIALIZATIONS_FETCHER_LIMIT', '100'))

RETRY_LIMIT = int(os.getenv('RETRY_LIMIT', '100'))




# REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
# REDIS_PORT = int(os.environ.get('REDIS_PORT', '6379'))
# REDIS_PASSWORD = os.environ.get('REDIS_PASSWORD', 'admin')
# REDIS_DB = int(os.environ.get('REDIS_DB', '0'))

