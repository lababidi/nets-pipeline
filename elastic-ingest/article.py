import urllib2
import json
from time import sleep


class Article:
    def __init__(self):
        self._url = None
        self._title = None
        self._publisher = None
        self._language = None
        self._content = None
        self._date_published_original = None
        self._date_published = None
        self._date_collected = None

    @property
    def url(self): return self._url

    @url.setter
    def url(self, v): self._url = v

    @property
    def title(self): return self._title

    @title.setter
    def title(self, v): self._title = v

    @property
    def publisher(self): return self._publisher

    @publisher.setter
    def publisher(self, v): self._publisher = v

    @property
    def language(self): return self._language

    @language.setter
    def language(self, v): self._language = v

    @property
    def content(self): return self._content

    @content.setter
    def content(self, v): self._content = v

    @property
    def date_published_original(self): return self._date_published_original

    @date_published_original.setter
    def date_published_original(self, v): self._date_published_original = v

    @property
    def date_published(self): return self._date_published

    @date_published.setter
    def date_published(self, v): self._date_published = v

    @property
    def date_collected(self): return self._date_collected

    @date_collected.setter
    def date_collected(self, v): self._date_collected = v
