# -*- coding: utf-8 -*-
from __future__ import absolute_import

from celery import Celery
from celery.utils.log import get_task_logger
from datetime import timedelta
from crawler import Crawler
from pymongo import MongoClient
from requests.exceptions import ConnectionError


app = Celery('tasks', broker='amqp://', backend='amqp://')
app.conf.update(CELERY_TIMEZONE='America/Sao_Paulo')
app.conf.update(
    CELERYBEAT_SCHEDULE={
        'run-consumidor-gov-br-update-database-every-day': {
            'task': 'tasks.consumidor_gov_br_update_database',
            'schedule': timedelta(days=1),
            'options': {'queue': 'update'},
        },
    },
)

DEFAULT_HEADER = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0',
}


@app.task(max_retries=3)
def check_data_file():
    """
    This task checks if data files exist on specified directory and process all crawling pages to access some links
    and post data on form.
    """
    url = 'http://gmail.com'

    data_file = '/home/luiz.ferreira/projetos/crawler/data/sample.json'
    links = ['link-signup']
    form = 'createaccount'

    try:
        Crawler().post_form(url=url, data_file=data_file, header=DEFAULT_HEADER, links=links, form=form)
    except Exception as exc:
        try:
            raise check_data_file.retry(exc=exc, countdown=30)
        except ConnectionError:
            return None


@app.task(max_retries=5)
def consumidor_gov_br_update_database():
    """
    This function make a crawler on https://www.consumidor.gov.br/pages/principal/empresas-participantes and save
    some urls on Mongodb collection named 'consumidor.empresas' to be used by external applications.
    """

    client = MongoClient()
    db = client.consumidor
    total_removed = db.empresas.count()
    db.empresas.remove()
    table = db.empresas

    url = 'https://www.consumidor.gov.br/pages/principal/empresas-participantes'

    try:
        html = Crawler().get(url=url, headers=DEFAULT_HEADER)
    except Exception as exc:
        try:
            raise consumidor_gov_br_update_database.retry(exc=exc, countdown=10)
        except ConnectionError:
            return None

    categories = html.find_all('a', {'class': 'accordion-toggle'})

    for category in categories:
        links = html.find_all(id=category.get('href')[1:])
        for item in links[0].find_all('a'):
            data = {'url': item.get('href'), 'name': item.text, 'category': category.text}
            table.insert_one(data).inserted_id

    logger = get_task_logger(__name__)
    logger.info('Total Removed:{} | Total Added:{}'.format(total_removed, table.count()))
