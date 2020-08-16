from app.flow import commons
from app.function.donwload_receita import get_files, download_from_url
import luigi, os, joblib
from pycarol.pipeline import inherit_list, ParquetTarget, CDSTarget
from app.flow import ingestion
from app.function import parser


luigi.auto_namespace(scope=__name__)


@inherit_list(
    ingestion.DownlaodFileReceita,
    ingestion.IngestCnaeInfo
)
class ParseFile(commons.Task):

    #allow only 2 parsers each time.
    resources = {'cpu': 5}
    download_link = commons.Parameter()
    file_creation = commons.Parameter()
    file_size = commons.Parameter()
    prefix_company = commons.Parameter()
    prefix_partner = commons.Parameter()
    prefix_cnae = commons.Parameter()

    def easy_run(self, inputs):
        file, subclass_cnaes = inputs
        return parser.parse_file(file, prefix_company=self.prefix_company,
                                 prefix_cnae=self.prefix_cnae,
                                 prefix_partner=self.prefix_partner,
                                 subclass_cnaes=subclass_cnaes)


@inherit_list(
    ParseFile
)
class SplitFiles(commons.Task):

    resources = {'cpu': 5}
    download_link = commons.Parameter()
    file_creation = commons.Parameter()
    file_size = commons.Parameter()
    staging = commons.Parameter()
    connector_name = commons.Parameter()
    record_type = commons.Parameter()

    def easy_run(self, inputs):
        return inputs[0].get(self.record_type)
