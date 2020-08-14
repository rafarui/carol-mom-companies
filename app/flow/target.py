from pycarol.pipeline import CDSTarget
import os

class ZipTarget(CDSTarget):
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