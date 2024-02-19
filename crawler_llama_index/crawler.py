import logging
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
from crawler_llama_index.paths import crawl_path
import time
import hashlib
import os
import sqlite3
from enum import Enum
from urllib.parse import urldefrag
import threading
from typing import Any, List, Callable


class status(Enum):
    UNKNOWN = 0
    FETCHED = 1
    EXISTS = 2
    
class Crawler(threading.Thread):

    def __init__(self, name: str, seed: str, wait: int, to_be_crawled: Callable[[str], bool], crawl_queue) -> None:
        super().__init__()
        self.name = name
        self.urls = [seed]
        self.path = crawl_path / name
        os.makedirs(self.path, exist_ok=True)
        self.wait = wait
        self.to_be_crawled = to_be_crawled
        #self._init_visited()
        self.visited = []
        self.crawl_queue = crawl_queue
        self.logger = logging.getLogger('Crawler')
        self.running = True

    # def _init_db(self):
    #     db_path = self.path / "db"
    #     self.db_conn = sqlite3.connect(db_path)
    #     self.db_conn.execute('CREATE TABLE IF NOT EXISTS docs (id VARCHAR(32) PRIMARY KEY, url TEXT, path VARCHAR(255), status INTEGER)')
    #     self.db_conn.commit()

    # def _init_visited(self):
    #     self.visitedAll = []
    #     cursor = self.db_conn.cursor()
    #     cursor.execute('SELECT url FROM docs WHERE 200 <= status < 300')
    #     for record in cursor.fetchall():
    #         self.visitedAll.append(record[0])
    #     cursor.close()

    def _remove_fragment_from_url(self, url: str) -> str:
        url_without_fragment, _ = urldefrag(url)
        return url_without_fragment
    
    def _fetch(self, url: str) -> Any:
        self.logger.info(f"Start fetch {url}")
        return requests.get(url)

    def _extract_links(self, base_url: str, html: str):
        soup = BeautifulSoup(html, 'html.parser')
        for link in soup.find_all('a'):
            new_link = link.get('href')
            if not new_link or new_link.startswith('#'):
                continue
            if new_link.startswith('/'):
                new_link = urljoin(base_url, new_link)
            new_link = self._remove_fragment_from_url(new_link)
            if not self.to_be_crawled(new_link) or new_link in self.visited:
                self.logger.debug(f"skip {new_link}")
                continue
            self.logger.debug(f"Extract new link {new_link}")
            yield new_link

    def _crawl(self, url: str) -> status:
        self.logger.info(f"start crawling {url}")

        result = status.UNKNOWN

        filepath = self._set_local_filepath(url)
        
        self.logger.debug(f"check if file exists {filepath}")
        html = ""
        if os.path.exists(filepath):
            self.logger.info(f"skip crawl, file exists {url}")
            with open(filepath, 'r') as file:
                html = file.read()
        if len(html):
            result = status.EXISTS
        else:
            response = self._fetch(url)
            html = response.text
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(html)
            # self.db_conn.execute('UPDATE docs SET status = ? WHERE id = ?', (response.status_code, self._get_id(url)))
            # self.db_conn.commit()
            result = status.FETCHED
        
        self.visited.append(url)
        
        # extract new linkds
        for url2 in self._extract_links(url, html):
            if url2 not in self.urls:
                self.logger.info(f"add new url to the list {url2}")
                self.urls.append(url2)
        
        self.crawl_queue.append((url, html))

        return result

    def _set_local_filepath(self, url: str) -> str:
        id = self._get_id(url)
        filename = f'page_{id}.html'
        abs_path = self.path / filename
        # self.db_conn.execute('INSERT OR IGNORE INTO docs (id, url, path, status) VALUES (?, ?, ?, ?)', (id, url, str(abs_path), 0))
        # self.db_conn.commit()
        return abs_path
    
    def _get_id(self, url: str) -> str:
        return hashlib.md5(url.encode()).hexdigest()

    def run(self):
        #self._init_db()
        while self.urls and self.running:
            self.logger.info(f'number of urls in the list: {len(self.urls)}')
            url = self.urls.pop(0)
            try:
                if self._crawl(url) == status.FETCHED:
                    self.logger.debug(f"waiting list {len(self.urls)}")
                    time.sleep(self.wait)
            except Exception:
                self.logger.exception(f'Failed to crawl: {url}')
                time.sleep(self.wait)

        #self.print_status()
    
    def stop(self):
        self.running = False
            
    
    # def print_status(self):
    #     self.logger.info("------- start print status -------")
    #     cursor = self.db_conn.cursor()
    #     cursor.execute('SELECT COUNT(*) FROM docs')
    #     for record in cursor.fetchall():
    #         self.logger.info(f"total count = {record[0]}")
    #     cursor.close()
    #     self.logger.info(f"urls waiting for crawl = {len(self.urls)}")
    #     self.logger.info("------- finish print status -------")
    
    # def close(self):
    #     self.db_conn.close()

