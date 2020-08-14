from app.flow import commons
from app.function.donwload_receita import get_files
import luigi, os, joblib
from pycarol.pipeline import inherit_list, ParquetTarget, CDSTarget
from app.flow import parser
from app.function import send

luigi.auto_namespace(scope=__name__)


class GetFiles(commons.Task):
    target_type = ParquetTarget
    server_ip = commons.Parameter()
    prefix_file = commons.Parameter()
    execution_timestamp = commons.Parameter()

    def easy_run(self, inputs):
        return get_files(url=self.server_ip, prefix_file=self.prefix_file)


@inherit_list(
    parser.SplitFiles
)
class SendToCarol(commons.Task):
    download_link = commons.Parameter()
    file_creation = commons.Parameter()
    file_size = commons.Parameter()
    staging = commons.Parameter()
    connector_name = commons.Parameter()
    record_type = commons.Parameter()

    def easy_run(self, inputs):
        return send.send_to_carol(inputs[0],
                                  staging_name=self.staging,
                                  connector_name=self.connector_name)


@inherit_list(
    GetFiles
)
class DynamicSend(commons.Task):

    server_ip = commons.Parameter()

    prefix_company = commons.Parameter()
    prefix_partner = commons.Parameter()
    prefix_cnae = commons.Parameter()

    def easy_run(self, inputs):
        data = inputs[0]

        files = []
        files.extend([
            SendToCarol(
                download_link=self.server_ip + row.link,
                file_creation=row.update,
                file_size=row.size,
                staging=commons.staging_company,
                connector_name=commons.connector_company,
                record_type=self.prefix_company,
                **self.get_execution_params(),
            ) for row in data.itertuples(index=False)
        ])

        files.extend([
            SendToCarol(
                download_link=self.server_ip + row.link,
                file_creation=row.update,
                file_size=row.size,
                staging=commons.staging_sec_cnae_,
                connector_name=commons.connector_sec_cnae,
                record_type=self.prefix_cnae,
                **self.get_execution_params(),
            ) for row in data.itertuples(index=False)
        ])

        files.extend([
            SendToCarol(
                download_link=self.server_ip + row.link,
                file_creation=row.update,
                file_size=row.size,
                staging=commons.staging_partner,
                connector_name=commons.connector_partner,
                record_type=self.prefix_partner,
                **self.get_execution_params(),
            ) for row in data.itertuples(index=False)
        ])

        yield files
        return files
