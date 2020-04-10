from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import urllib.request, json
import gzip


def fetch_and_extract_covid_data_from_rki():
    offset = 0
    covid_data = []
    exceeded_transfer_limit = True
    # sue pagination to get all covid data from rki
    while exceeded_transfer_limit:
        with urllib.request.urlopen(
            f"https://services7.arcgis.com/mOBPykOjAyBO2ZKk/arcgis/rest/services/RKI_COVID19/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json&resultOffset={offset}"
        ) as url:
            data = json.loads(url.read().decode())
            # is still data missing?
            exceeded_transfer_limit = "exceededTransferLimit" in data
            # extract relevant data from json
            data = [e["attributes"] for e in data["features"]]
            offset += 2000
            covid_data.extend(data)
    covid_data = "\n".join([json.dumps(f) for f in covid_data])
    return covid_data


class CovidApiToS3Operator(BaseOperator):
    ui_color = "#ccccff"
    template_fields = ("s3_key",)

    @apply_defaults
    def __init__(
        self,
        aws_credentials_id="aws_credentials",
        s3_bucket="",
        s3_key="",
        date=None,
        *args,
        **kwargs,
    ):

        super(CovidApiToS3Operator, self).__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.date = date

    def execute(self, context):

        s3_hook = S3Hook(self.aws_credentials_id)

        covid_data = fetch_and_extract_covid_data_from_rki()
        compressed_value = gzip.compress(bytes(covid_data, "utf-8"))
        s3_hook.load_bytes(
            compressed_value,
            f"{self.s3_key}/{self.date.strftime('%Y-%m-%d')}.json.gz",
            bucket_name=self.s3_bucket,
            replace=True,
        )


class AirPollutionApiToS3Operator(BaseOperator):
    ui_color = "#358140"

    polutant_info = {
        "PM10": {"id": 1, "eval_id": 1},
        "CO": {"id": 2, "eval_id": 4},
        "Ozon": {"id": 3, "eval_id": 4},
        "SO2": {"id": 4, "eval_id": 1},
        "NO2": {"id": 5, "eval_id": 2},
    }

    @apply_defaults
    def __init__(
        self,
        date=None,
        aws_credentials_id="aws_credentials",
        s3_bucket="",
        s3_key="",
        pollutant="",
        *args,
        **kwargs,
    ):

        super(AirPollutionApiToS3Operator, self).__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.pollutant = pollutant
        self.date = date

    def execute(self, context):

        s3_hook = S3Hook(self.aws_credentials_id)
        pollutant_id = AirPollutionApiToS3Operator.polutant_info[self.pollutant]["id"]
        pollutant_eval_id = AirPollutionApiToS3Operator.polutant_info[self.pollutant][
            "eval_id"
        ]
        with urllib.request.urlopen(
            f"https://www.umweltbundesamt.de/api/air_data/v2/measures/csv?date_from={self.date.strftime('%Y-%m-%d')}&time_from=12&date_to={self.date.strftime('%Y-%m-%d')}&time_to=12&data%5B0%5D%5Bco%5D={pollutant_id}&data%5B0%5D%5Bsc%5D={pollutant_eval_id}&lang=de"
        ) as url:
            data = url.read()

        s3_hook.load_bytes(
            gzip.compress(data),
            f"{self.s3_key}/{self.pollutant}/{self.date.strftime('%Y-%m-%d')}.csv.gz",
            bucket_name=self.s3_bucket,
            replace=True,
        )
