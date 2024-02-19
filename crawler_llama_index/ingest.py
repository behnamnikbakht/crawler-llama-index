import logging
import pickle
import threading
from abc import ABC, abstractmethod
from typing import List
from dataclasses import dataclass
from llama_index import Document
from llama_index.readers import StringIterableReader
import hashlib
from crawler_llama_index.paths import index_path
from crawler_llama_index.index import Indexer
import time
from llama_index.readers.base import BaseReader
from typing import TYPE_CHECKING, Any, Dict, Iterable, List

@dataclass
class ParseRecord:
    url: str
    title: str
    content: str

class Parser(ABC):
    @abstractmethod
    def parse(self, url: str, html: str) -> ParseRecord:
        pass

class CrawlReader(BaseReader):
    def __init__(self, crawl_queue, parser: Parser) -> None:
        super().__init__()
        self.crawl_queue = crawl_queue
        self.parser = parser
        self.logger = logging.getLogger('CrawlReader')
        self.dedup = dedup()

    def _exclude_metadata(self, documents: List[Document]) -> List[Document]:
        for document in documents:
            document.metadata["doc_id"] = document.doc_id
            document.excluded_embed_metadata_keys.extend(["doc_id", "url"])
            document.excluded_llm_metadata_keys.extend(["doc_id", "url"])
        return documents

    def load_data(self, *args: Any, **load_kwargs: Any) -> List[Document]:
        documents = []
        while self.crawl_queue:
            url, page = self.crawl_queue.pop(0)
            record = self.parser.parse(url, page)
            if record.content is None or not len(record.content):
                self.logger.info(f"skip - empty content {url}")
                continue
            if self.dedup.exists(record.content):
                self.logger.info(f"skip - already read {url}")
                continue
            for doc in self._read(record):
                self.logger.debug(f"loaded {url}")
                documents.append(doc)
        return documents
    
    def _read(self, record):
        string_reader = StringIterableReader()
        documents = string_reader.load_data([record.content])
        for doc in documents:
            doc.metadata['title'] = record.title
            doc.metadata['url'] = record.url
        self._exclude_metadata(documents)
        return documents

class dedup:
    def __init__(self) -> None:
        self.ds = set()

    def exists(self, content):
        h = self._calculate_md5_hash(content)
        if h in self.ds:
            return True
        self.ds.add(h)
        return False
    
    def _calculate_md5_hash(self, input_string):
        md5_hash = hashlib.md5()
        md5_hash.update(input_string.encode('utf-8'))
        hash_result = md5_hash.hexdigest()
        return hash_result


class Ingestor(BaseReader):
    def __init__(self, reader: BaseReader) -> None:
        super().__init__()
        self.logger = logging.getLogger('Ingestor')
        self.reader = reader
    
    def load_data(self) -> List[Document]:
        return self.reader.load_data()

    def lazy_load_data(self) -> Iterable[Document]:
        return self.reader.lazy_load_data()

