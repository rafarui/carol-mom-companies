import requests

from app.flow import commons
from app.function.donwload_receita import get_files, download_from_url
import luigi, os, joblib
from pycarol.pipeline import inherit_list, ParquetTarget, CDSTarget


luigi.auto_namespace(scope=__name__)

class FileTarget(CDSTarget):
    """
    This target operates with filepaths.
    easy_run should return a filepath for a local temporary file. This file will be removed after been sent to Carol.
    When loading this target, the file is copied from Carol to a local file. On easy_run we receive the local filepath.
    Important note: when loading the target, its local copy will not be automatically removed.
    """

    FILE_EXT = 'zip'

    def load_cds(self):
        return self.storage.load(self.path, format='file', cache=False)

    def dump_cds(self, tempfile_path):
        self.storage.save(self.path, tempfile_path, format='file', cache=False)
        os.remove(tempfile_path)

    def load_local(self):
        return self.path

    def dump_local(self, tempfile_path):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        os.rename(tempfile_path, self.path)

class getFiles(commons.Task):
    target_type = ParquetTarget
    server_ip = commons.Parameter()
    prefix_file = commons.Parameter()
    execution_timestamp = commons.Parameter()

    def easy_run(self, inputs):
        return get_files(url=self.server_ip, prefix_file=self.prefix_file)


class donwlaodFile(commons.Task):

    target_type = FileTarget
    download_link = commons.Parameter()
    file_creation = commons.Parameter()
    file_size = commons.Parameter()

    def easy_run(self, inputs):
        return download_from_url(url=self.download_link)

class parseFile(commons.Task):

    target_type = FileTarget
    download_link = commons.Parameter()
    file_creation = commons.Parameter()
    file_size = commons.Parameter()

    def easy_run(self, inputs):
        return download_from_url(url=self.download_link)




@inherit_list(
    getFiles
)
class DynamicDownload(commons.Task):

    server_ip = commons.Parameter()
    def easy_run(self, inputs):

        data = inputs[0]

        files = [
            donwlaodFile(
                download_link=self.server_ip + row.link,
                file_creation=row.update,
                file_size=row.size, **self.get_execution_params(),
            ) for row in data.itertuples(index=False)
        ]
        yield files
        return files
