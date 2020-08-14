import sys, os, shutil
import luigi

luigi.interface.InterfaceLogging.setup(luigi.interface.core())
import datetime
import os, time
import logging
import traceback
logger = logging.getLogger(__name__)

from pycarol.carol import Carol
from pycarol.pipeline import Task, inherit_list, ParquetTarget
from pycarol.apps import Apps

from luigi import Parameter

is_cloud_target = os.environ.get('CLOUDTARGET', 'True')
if is_cloud_target == 'False':
    is_cloud_target = False
else:
    is_cloud_target = True


PROJECT_PATH = os.getcwd()
TARGET_PATH = os.path.join(PROJECT_PATH, 'luigi_targets')
Task.TARGET_DIR = TARGET_PATH
Task.is_cloud_target = is_cloud_target
Task.version = Parameter()
Task.tenant = Parameter()
Task.resources = {'cpu': 1}
max_cpu = 30

# https://receita.economia.gov.br/orientacao/tributaria/cadastros/cadastro-nacional-de-pessoas-juridicas-cnpj/dados-publicos-cnpj
server_ip = 'http://200.152.38.155/CNPJ/'
prefix_file = 'DADOS_ABERTOS_CNPJ'
# prefix_file = "LAYOUT_DADOS_ABERTOS"

@Task.event_handler(luigi.Event.FAILURE)
def mourn_failure(task, exception):
    """Will be called directly after a failed execution
       of `run` on any JobTask subclass
    """
    logger.error(f'Error msg: {exception} ---- Error: Task {task},')
    traceback_str = ''.join(traceback.format_tb(exception.__traceback__))
    logger.error(traceback_str, )


@Task.event_handler(luigi.Event.PROCESSING_TIME)
def print_execution_time(self, processing_time):
    logger.debug(f'### PROCESSING TIME {processing_time}s. Output saved at {self.output().path}')


#######################################################################################################

# get app settings

login = Carol()
params = dict(
    tenant=os.environ['CAROLTENANT'],  # carol tenant
    version=os.environ['CAROLAPPVERSION'],  # model version
    execution_timestamp="1597396486.87124", #time.time(),  # execution timestamp
    server_ip=server_ip,
    prefix_file=prefix_file,
)