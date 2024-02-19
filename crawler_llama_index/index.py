import logging
from llama_index.indices.base import BaseIndex
from llama_index.data_structs import IndexDict
from llama_index import (
    Document,
    ServiceContext,
    StorageContext,
    VectorStoreIndex,
    load_index_from_storage,
)
from crawler_llama_index.paths import index_path
import os
from llama_index.embeddings import LangchainEmbedding
from langchain.embeddings.huggingface import HuggingFaceBgeEmbeddings
from llama_index.node_parser import SentenceWindowNodeParser
from llama_index.indices.vector_store.base import VectorStoreIndex
from llama_index.readers.base import BaseReader

class Indexer:
    def __init__(self, name:str, data_loader:BaseReader=None) -> None:
        super().__init__()
        self.path = index_path / name
        self.data_loader = data_loader
        self.logger = logging.getLogger('Indexer')
        self.index, self.service_context = self._initialize_index()
    
    def _initialize_index(self) -> BaseIndex[IndexDict]:
        embed_model = LangchainEmbedding(HuggingFaceBgeEmbeddings(model_name="BAAI/bge-large-en-v1.5"))
        node_parser = SentenceWindowNodeParser.from_defaults(
            window_size=3,
            window_metadata_key="window",
            original_text_metadata_key="original_text",
        )
        #service_context = ServiceContext.from_defaults(embed_model=self.embed_model, llm=None)
        service_context = ServiceContext.from_defaults(embed_model=embed_model, node_parser=node_parser, llm=None)
        if os.path.exists(self.path):
            self.logger.info("try to load index")
            storage_context = StorageContext.from_defaults(persist_dir=self.path)
            index = load_index_from_storage(
                storage_context=storage_context,
                service_context=service_context,
                store_nodes_override=True,
                show_progress=True,
            )
            self.logger.info(f"number of docs = {len(index.docstore.get_all_document_hashes())}")
        else:
            os.makedirs(self.path, exist_ok=True)
            self.logger.info("create a new index")
            index = VectorStoreIndex.from_documents(documents=self.data_loader.load_data(), service_context=service_context)
            index.storage_context.persist(persist_dir=self.path)
        self.logger.info("finish loading index")
        return index, service_context
    
    # def save_index(self) -> None:
    #     self.logger.info("save index")
    #     self.index.storage_context.persist(persist_dir=self.path)
    
    def query(self, question):
        query_engine = self.index.as_query_engine(similarity_top_k=2)
        response = query_engine.query(question)
        self.logger.info(f"response = {response}\n\n")
        #nodes = self.index.as_retriever().retrieve(question)
        #self.logger.info(nodes)
    
    # def insert(self, document):
    #     self.logger.info("index document")
    #     self.index.insert(document=document, service_context=self.service_context, show_progress=False)


