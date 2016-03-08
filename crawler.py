# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
import json
import requests


class Crawler(object):

    def get(self, url, headers=None):
        """
        Makes a GET request that returns text/html and json results or raise an exception with request status code.
        """
        response = requests.get(url, headers)
        if response.status_code == 200:
            return BeautifulSoup(response.text, 'html.parser')

        raise Exception('GET request not found! HTTP Status Code is {}'.format(response.status_code))

    def post(self, url, data=None, headers=None, session=False):
        """
        Makes a POST request with optional requests.Session param that returns tex/html and json results or raise an
        exception with request status code.
        """
        request = requests
        if session:
            request = request.Session()
        response = request.post(url, data=data, headers=headers)

        if response.status_code == 200:
            return response

        raise Exception('POST request not found! HTTP Status Code is {}'.format(response.status_code))

    def load_data_file(self, data_file):
        """
        Opens data file type like json or txt and return a python object.
        """
        with open('{}'.format(data_file)) as data:
                return json.load(data)

    def post_form(self, url=None, header=None, data=None, data_file=None, links=None, form=None, method=None):
        """
        Opens url to check some links/forms to proceed to a new step, find the html form ID to post data/data_file on
        this form.
        """

        if not data and data_file:
            data = self.load_data_file(data_file)

        if not links:
            links = []

        for link in links:
            while True:
                html = self.get(url)
                if html.find(id=form):
                    break

                if html.find(id=link):
                    url = html.find(id=link).get('href')
                    continue
                break

        for item in data:
            result = self.post(url, headers=header, data=item, session=True)
            print(BeautifulSoup(result.text, 'html.parser').prettify())
