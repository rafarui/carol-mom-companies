import sys, os, shutil
import luigi

from dotenv import load_dotenv

load_dotenv('.env', override=True)

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


mock_file =  os.environ.get('MOCKFILE', None)

PROJECT_PATH = os.getcwd()
TARGET_PATH = os.path.join(PROJECT_PATH, 'luigi_targets')
Task.TARGET_DIR = TARGET_PATH
Task.is_cloud_target = is_cloud_target
Task.version = Parameter()
Task.tenant = Parameter()
Task.resources = {'cpu': 1}

# https://receita.economia.gov.br/orientacao/tributaria/cadastros/cadastro-nacional-de-pessoas-juridicas-cnpj/dados-publicos-cnpj
server_ip = 'http://200.152.38.155/CNPJ/'
prefix_file = 'DADOS_ABERTOS_CNPJ'
# prefix_file = "LAYOUT_DADOS_ABERTOS"

connector_name = 'receita_federal'

staging_partner = 'partner_receita'
connector_partner = connector_name

staging_company = 'company_receita'
connector_company = connector_name

staging_sec_cnae_ = 'secondary_cnae_receita'
connector_sec_cnae = connector_name

# mapping cnae
staging_cnae_mapping = 'cnae_information'
connector_cnae_mapping = 'receita_federal'

# prefix
prefix_company = 'company'
prefix_partner = 'partners'
prefix_cnae = 'secondary_cnaes'


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
    execution_timestamp="1597396486.87124",  # time.time(),  # execution timestamp
    server_ip=server_ip,
    prefix_file=prefix_file,
    staging_cnae_mapping=staging_cnae_mapping,
    connector_cnae_mapping=connector_cnae_mapping,
    prefix_company=prefix_company,
    prefix_partner=prefix_partner,
    prefix_cnae=prefix_cnae,
    mock_file=mock_file,
)
