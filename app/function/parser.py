# -*- encoding: utf-8 -*-
import pyarrow as pa
import pyarrow.parquet as pq

import pandas as pd
from app.function.cfwf import read_cfwf

from app.function.mappings_parser import (
    situacao_mapping, company_size, matriz_filial_mapping,
    cadastral_situation_description_mapping, tipo_socio_mapping,
    natureza_mapping
)

import logging
import os

logger = logging.getLogger(__name__)

# Nome das colunas de empresas
EMP_CNPJ = 'cnpj'
EMP_MATRIZ_FILIAL = 'matriz_filial'
EMP_RAZAO_SOCIAL = 'razao_social'
EMP_NOME_FANTASIA = 'nome_fantasia'
EMP_SITUACAO = 'situacao'
EMP_DATA_SITUACAO = 'data_situacao'
EMP_MOTIVO_SITUACAO = 'motivo_situacao'
EMP_NM_CIDADE_EXTERIOR = 'nm_cidade_exterior'
EMP_COD_PAIS = 'cod_pais'
EMP_NOME_PAIS = 'nome_pais'
EMP_COD_NAT_JURIDICA = 'cod_nat_juridica'
EMP_DATA_INICIO_ATIV = 'data_inicio_ativ'
EMP_CNAE_FISCAL = 'cnae_fiscal'
EMP_TIPO_LOGRADOURO = 'tipo_logradouro'
EMP_LOGRADOURO = 'logradouro'
EMP_NUMERO = 'numero'
EMP_COMPLEMENTO = 'complemento'
EMP_BAIRRO = 'bairro'
EMP_CEP = 'cep'
EMP_UF = 'uf'
EMP_COD_MUNICIPIO = 'cod_municipio'
EMP_MUNICIPIO = 'municipio'
EMP_DDD_1 = 'ddd_1'
EMP_TELEFONE_1 = 'telefone_1'
EMP_DDD_2 = 'ddd_2'
EMP_TELEFONE_2 = 'telefone_2'
EMP_DDD_FAX = 'ddd_fax'
EMP_NUM_FAX = 'num_fax'
EMP_EMAIL = 'email'
EMP_QUALIF_RESP = 'qualif_resp'
EMP_CAPITAL_SOCIAL = 'capital_social'
EMP_PORTE = 'porte'
EMP_OPC_SIMPLES = 'opc_simples'
EMP_DATA_OPC_SIMPLES = 'data_opc_simples'
EMP_DATA_EXC_SIMPLES = 'data_exc_simples'
EMP_OPC_MEI = 'opc_mei'
EMP_SIT_ESPECIAL = 'sit_especial'
EMP_DATA_SIT_ESPECIAL = 'data_sit_especial'

# Nome das colunas de socios
SOC_CNPJ = 'cnpj'
SOC_TIPO_SOCIO = 'tipo_socio'
SOC_NOME_SOCIO = 'nome_socio'
SOC_CNPJ_CPF_SOCIO = 'cnpj_cpf_socio'
SOC_COD_QUALIFICACAO = 'cod_qualificacao'
SOC_PERC_CAPITAL = 'perc_capital'
SOC_DATA_ENTRADA = 'data_entrada'
SOC_COD_PAIS_EXT = 'cod_pais_ext'
SOC_NOME_PAIS_EXT = 'nome_pais_ext'
SOC_CPF_REPRES = 'cpf_repres'
SOC_NOME_REPRES = 'nome_repres'
SOC_COD_QUALIF_REPRES = 'cod_qualif_repres'

# Nome das colunas de cnaes secundarios
CNA_CNPJ = 'cnpj'
CNA_CNAE = 'cnae'
CNA_ORDEM = 'cnae_ordem'
# FIM DAS CONSTANTES PARA DEFINICAO DE NOMENCLATURA

EMPRESAS_COLUNAS = [
    (EMP_CNPJ, (3, 17)),
    (EMP_MATRIZ_FILIAL, (17, 18)),
    (EMP_RAZAO_SOCIAL, (18, 168)),
    (EMP_NOME_FANTASIA, (168, 223)),
    (EMP_SITUACAO, (223, 225)),
    (EMP_DATA_SITUACAO, (225, 233)),
    (EMP_MOTIVO_SITUACAO, (233, 235)),
    (EMP_NM_CIDADE_EXTERIOR, (235, 290)),
    (EMP_COD_PAIS, (290, 293)),
    (EMP_NOME_PAIS, (293, 363)),
    (EMP_COD_NAT_JURIDICA, (363, 367)),
    (EMP_DATA_INICIO_ATIV, (367, 375)),
    (EMP_CNAE_FISCAL, (375, 382)),
    (EMP_TIPO_LOGRADOURO, (382, 402)),
    (EMP_LOGRADOURO, (402, 462)),
    (EMP_NUMERO, (462, 468)),
    (EMP_COMPLEMENTO, (468, 624)),
    (EMP_BAIRRO, (624, 674)),
    (EMP_CEP, (674, 682)),
    (EMP_UF, (682, 684)),
    (EMP_COD_MUNICIPIO, (684, 688)),
    (EMP_MUNICIPIO, (688, 738)),
    (EMP_DDD_1, (738, 742)),
    (EMP_TELEFONE_1, (742, 750)),
    (EMP_DDD_2, (750, 754)),
    (EMP_TELEFONE_2, (754, 762)),
    (EMP_DDD_FAX, (762, 766)),
    (EMP_NUM_FAX, (766, 774)),
    (EMP_EMAIL, (774, 889)),
    (EMP_QUALIF_RESP, (889, 891)),
    (EMP_CAPITAL_SOCIAL, (891, 905)),
    (EMP_PORTE, (905, 907)),
    (EMP_OPC_SIMPLES, (907, 908)),
    (EMP_DATA_OPC_SIMPLES, (908, 916)),
    (EMP_DATA_EXC_SIMPLES, (916, 924)),
    (EMP_OPC_MEI, (924, 925)),
    (EMP_SIT_ESPECIAL, (925, 948)),
    (EMP_DATA_SIT_ESPECIAL, (948, 956))
]

EMPRESAS_DTYPE = {EMP_CAPITAL_SOCIAL: float}

SOCIOS_COLUNAS = [
    (SOC_CNPJ, (3, 17)),
    (SOC_TIPO_SOCIO, (17, 18)),
    (SOC_NOME_SOCIO, (18, 168)),
    (SOC_CNPJ_CPF_SOCIO, (168, 182)),
    (SOC_COD_QUALIFICACAO, (182, 184)),
    (SOC_PERC_CAPITAL, (184, 189)),
    (SOC_DATA_ENTRADA, (189, 197)),
    (SOC_COD_PAIS_EXT, (197, 200)),
    (SOC_NOME_PAIS_EXT, (200, 270)),
    (SOC_CPF_REPRES, (270, 281)),
    (SOC_NOME_REPRES, (281, 341)),
    (SOC_COD_QUALIF_REPRES, (341, 343))
]

SOCIOS_DTYPE = {SOC_PERC_CAPITAL: float}

CNAES_COLNOMES = [CNA_CNPJ] + [num for num in range(99)]
CNAES_COLSPECS = [(3, 17)] + [(num * 7 + 17, num * 7 + 24) for num in range(99)]

HEADER_COLUNAS = [
    ('Nome do arquivo', (17, 28)),
    ('Data de gravacao', (28, 36)),
    ('Numero da remessa', (36, 44))
]

TRAILLER_COLUNAS = [
    ('Total de registros de empresas', (17, 26)),
    ('Total de registros de socios', (26, 35)),
    ('Total de registros de CNAEs secundarios', (35, 44)),
    ('Total de registros incluindo header e trailler', (44, 55))
]

CHUNKSIZE = 200000


def process_empresa(df, subclass_cnaes=None):
    logger.debug(f'Processing batch "empresa" {df.shape}')
    # Troca datas zeradas por vazio
    df[EMP_DATA_OPC_SIMPLES] = (df[EMP_DATA_OPC_SIMPLES]
                                .where(df[EMP_DATA_OPC_SIMPLES] != '00000000', ''))
    df[EMP_DATA_EXC_SIMPLES] = (df[EMP_DATA_EXC_SIMPLES]
                                .where(df[EMP_DATA_EXC_SIMPLES] != '00000000', ''))
    df[EMP_DATA_SIT_ESPECIAL] = (df[EMP_DATA_SIT_ESPECIAL]
                                 .where(df[EMP_DATA_SIT_ESPECIAL] != '00000000', ''))

    df['situacao_description'] = df['situacao'].map(situacao_mapping)
    df['porte_description'] = df['porte'].map(company_size)
    df['matriz_filial_description'] = df['matriz_filial'].map(matriz_filial_mapping)
    df['motivo_situacao_description'] = df['motivo_situacao'].map(cadastral_situation_description_mapping)
    df['cod_nat_juridica_description'] = df['cod_nat_juridica'].map(natureza_mapping)

    if subclass_cnaes is not None:
        df['cnae_fiscal_description'] = df['cnae_fiscal'].map(subclass_cnaes)

    return df.reset_index(drop=True)


def process_socios(df):
    logger.debug(f'Processing batch "socios" {df.shape}')

    # Troca cpf invalido por vazio
    df[SOC_CPF_REPRES] = (df[SOC_CPF_REPRES]
                          .where(df[SOC_CPF_REPRES] != '***000000**', ''))
    df[SOC_NOME_REPRES] = (df[SOC_NOME_REPRES]
                           .where(df[SOC_NOME_REPRES] != 'CPF INVALIDO', ''))

    # Se socio for tipo 1 (cnpj), deixa campo intacto, do contrario,
    # fica apenas com os ultimos 11 digitos
    df[SOC_CNPJ_CPF_SOCIO] = (df[SOC_CNPJ_CPF_SOCIO]
                              .where(df[SOC_TIPO_SOCIO] == '1',
                                     df[SOC_CNPJ_CPF_SOCIO].str[-11:]))

    df['tipo_socio_description'] = df['tipo_socio'].map(tipo_socio_mapping)

    return df.reset_index(drop=True)


def process_cnaes(df, subclass_cnaes=None):
    logger.debug(f'Processing batch "cnaes" {df.shape}')
    # Verticaliza tabela de associacao de cnaes secundarios,
    # mantendo apenas os validos (diferentes de 0000000)
    df = pd.melt(df,
                 id_vars=[CNA_CNPJ],
                 value_vars=range(99),
                 var_name=CNA_ORDEM,
                 value_name=CNA_CNAE)

    df = df[df[CNA_CNAE] != '0000000']
    if subclass_cnaes is not None:
        df['cnae_sec_description'] = df['cnae'].map(subclass_cnaes)

    return df.reset_index(drop=True)


def parse_file(
        file, prefix_company='company',
        prefix_cnae='partners', prefix_partner='secondary_cnaes',
        subclass_cnaes=None,
):
    """
    Parse a Zip File from Receita Federal

    Args:
        file: `str`
            file to parse
        subclass_cnaes: `dict` default None
            Dictionary of {cnae_code : description} to add to the cnae
    Returns:
        dict {
        prefix_company: pd.DataFrame,
        prefix_cnae: pd.DataFrame,
        prefix_partner: pd.DataFrame,
        }

    """

    REGISTROS_TIPOS = {
        '1': prefix_company,
        '2': prefix_partner,
        '6': prefix_cnae
    }

    total_empresas = 0
    total_socios = 0
    total_cnaes = 0

    all_files = {}
    header_colnomes = list(list(zip(*HEADER_COLUNAS))[0])
    empresas_colnomes = list(list(zip(*EMPRESAS_COLUNAS))[0])
    socios_colnomes = list(list(zip(*SOCIOS_COLUNAS))[0])
    trailler_colnomes = list(list(zip(*TRAILLER_COLUNAS))[0])

    header_colspecs = list(list(zip(*HEADER_COLUNAS))[1])
    empresas_colspecs = list(list(zip(*EMPRESAS_COLUNAS))[1])
    socios_colspecs = list(list(zip(*SOCIOS_COLUNAS))[1])
    trailler_colspecs = list(list(zip(*TRAILLER_COLUNAS))[1])

    logger.debug(f'Processing file: {file}')
    dados = read_cfwf(
        file,
        type_width=1,
        colspecs={'0': header_colspecs,
                  '1': empresas_colspecs,
                  '2': socios_colspecs,
                  '6': CNAES_COLSPECS,
                  '9': trailler_colspecs},
        names={'0': header_colnomes,
               '1': empresas_colnomes,
               '2': socios_colnomes,
               '6': CNAES_COLNOMES,
               '9': trailler_colnomes},
        dtype={'1': EMPRESAS_DTYPE,
               '2': SOCIOS_DTYPE},
        chunksize=CHUNKSIZE,
        encoding='ISO-8859-15'
    )

    # Itera sobre blocos (chunks) do arquivo
    parquet_writers = {}
    for i_bloco, bloco in enumerate(dados):

        for tipo_registro, df in bloco.items():

            if tipo_registro == '1':  # empresas
                total_empresas += len(df)
                df = process_empresa(df, subclass_cnaes=subclass_cnaes)

            elif tipo_registro == '2':  # socios
                total_socios += len(df)
                df = process_socios(df)

            elif tipo_registro == '6':  # cnaes_secundarios
                total_cnaes += len(df)
                df = process_cnaes(df, subclass_cnaes=subclass_cnaes)

            elif tipo_registro == '0':  # header
                header = df.iloc[0, :]
                logger.info(f'Header Info {header}')
                continue

            elif tipo_registro == '9':  # trailler

                trailler = df.iloc[0, :]
                controle_empresas = int(trailler['Total de registros de empresas'])
                controle_socios = int(trailler['Total de registros de socios'])
                controle_cnaes = int(trailler['Total de registros de CNAEs secundarios'])
                total = int(trailler['Total de registros incluindo header e trailler'])
                logger.info(f'Records in company: {controle_empresas}')
                logger.info(f'Records in partners: {controle_socios}')
                logger.info(f'Records in CNAEs secundarios: {controle_cnaes}')
                logger.info(f'Total in the file: {total}')
                continue

            table = pa.Table.from_pandas(df)
            if i_bloco == 0:
                prefix_file = os.path.basename(file).split('.')[0]
                parquet_filename = f"{prefix_file}_{REGISTROS_TIPOS[tipo_registro]}.parquet"
                all_files.update({REGISTROS_TIPOS[tipo_registro]: parquet_filename})
                parquet_writers[REGISTROS_TIPOS[tipo_registro]] = pq.ParquetWriter(parquet_filename, table.schema)
            pqwriter = parquet_writers[REGISTROS_TIPOS[tipo_registro]]
            pqwriter.write_table(table)

    logger.info(f'Records in file: company: {total_empresas}')
    logger.info(f'Records in file: partners: {total_socios}')
    logger.info(f'Records in file: CNAEs secundarios: {total_cnaes}')

    for _, pqwriter in parquet_writers.items():
        if pqwriter:
            pqwriter.close()

    return {key: pd.read_parquet(value) for key, value in all_files.items()}
