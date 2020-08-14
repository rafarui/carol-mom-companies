from bs4 import BeautifulSoup
import requests
import pandas as pd
from tqdm import tqdm
import logging, os
logger = logging.getLogger(__name__)

def get_files(url, prefix_file='DADOS_ABERTOS_CNPJ'):

    """
    Parse the data from Receita website
    Args:
        url: `str`
            Receita Federal URL
        prefix_file: `str` default "prefix_file"
            Prefix of the files to get.

    Returns:
        pd.DataFrame with the columns link, update, size

    """

    r = requests.get(url)
    search = BeautifulSoup(r.text, features="html.parser")
    result = search.find_all('tr', )
    files_dict = []
    for t in result:
        tds = t.find_all('td')
        if len(tds) == 5:
            link = tds[1].a['href']
            update = tds[2].text.strip()
            size = tds[3].text.strip()
            files_dict.append(dict(
                link=link, update=update, size=size
            ))

    df = pd.DataFrame(files_dict)
    df = df[df.link.str.startswith(prefix_file)].reset_index(drop=True)
    return df


def download_from_url(url, chunk_size=1024):
    dst = url.split('/')[-1]
    chunk_size = int(chunk_size)
    file_size = int(requests.head(url).headers["Content-Length"])
    pbar = tqdm( total=file_size, unit='B', unit_scale=True, unit_divisor=1024, desc=url.split('/')[-1])
    req = requests.get(url, stream=True)
    with(open(dst, 'wb')) as f:
        for chunk in req.iter_content(chunk_size=chunk_size):
            if chunk:
                f.write(chunk)
                pbar.update(chunk_size)
    pbar.close()
    return dst