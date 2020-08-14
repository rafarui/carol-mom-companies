from app.flow import commons
from app.function.donwload_receita import download_from_url
import luigi
from app.function import ingestion
from app.flow.target import ZipTarget

luigi.auto_namespace(scope=__name__)

class IngestCnaeInfo(commons.Task):

    staging_cnae_mapping = commons.Parameter()
    connector_cnae_mapping = commons.Parameter()

    def easy_run(self, inputs):
        out = ingestion.ingest_cnae(
            self.staging_cnae_mapping, self.connector_cnae_mapping, max_workers=3,
            columns=['subclass_cnae','description_cnae'], merge_records=True,
            return_metadata=False,
        )
        return out


class DownlaodFileReceita(commons.Task):

    target_type = ZipTarget
    download_link = commons.Parameter()
    file_creation = commons.Parameter()
    file_size = commons.Parameter()
    mock_file = commons.Parameter(default=None)

    def easy_run(self, inputs):
        if self.mock_file is not None:
            return self.mock_file
        return download_from_url(url=self.download_link)