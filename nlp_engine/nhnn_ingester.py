"""
RAG Data Ingestion - Trạm nạp tài liệu NHNN
Sử dụng LangChain và ChromaDB để xử lý và lưu trữ tài liệu
"""

import os
import json
import logging
import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from pathlib import Path

# LangChain imports
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.document_loaders import PyPDFLoader, Docx2txtLoader, TextLoader
from langchain_community.vectorstores import Chroma
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain.schema import Document

# ChromaDB client
import chromadb
from chromadb.config import Settings

# Utilities
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from dotenv import load_dotenv
from tqdm import tqdm
import hashlib

# Load environment variables
load_dotenv()

class NHNNDocumentIngester:
    """Trạm nạp tài liệu NHNN với RAG processing"""
    
    def __init__(self):
        # Configuration
        self.chroma_host = os.getenv('AWS_CHROMA_HOST', 'localhost:8000')
        self.docs_dir = Path(os.getenv('NHNN_DOCS_DIR', 'nhnn_docs'))
        self.collection_name = os.getenv('COLLECTION_NAME', 'macro_policies')
        self.chunk_size = int(os.getenv('CHUNK_SIZE', 1000))
        self.chunk_overlap = int(os.getenv('CHUNK_OVERLAP', 200))
        self.batch_size = int(os.getenv('BATCH_SIZE', 10))
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger('NHNNIngester')
        
        # Initialize components
        self._setup_directories()
        self._setup_text_splitter()
        self._setup_embeddings()
        self._setup_chromadb()
        
        # Track processed files
        self.processed_files = set()
        self._load_processed_files()
        
        self.logger.info("NHNN Document Ingester initialized successfully")
    
    def _setup_directories(self):
        """Tạo thư mục cần thiết"""
        self.docs_dir.mkdir(exist_ok=True)
        
        # Create subdirectories for different document types
        (self.docs_dir / 'pdf').mkdir(exist_ok=True)
        (self.docs_dir / 'docx').mkdir(exist_ok=True)
        (self.docs_dir / 'txt').mkdir(exist_ok=True)
        
        self.logger.info(f"Documents directory: {self.docs_dir.absolute()}")
    
    def _setup_text_splitter(self):
        """Cài đặt text splitter cho chunking"""
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=self.chunk_size,
            chunk_overlap=self.chunk_overlap,
            length_function=len,
            separators=[
                "\n\n",  # Double newlines
                "\n",    # Single newlines
                ".",     # Sentences
                "!",     # Exclamations
                "?",     # Questions
                ";",     # Semicolons
                ",",     # Commas
                " ",     # Spaces
                ""       # Characters
            ]
        )
        self.logger.info(f"Text splitter configured: chunk_size={self.chunk_size}, overlap={self.chunk_overlap}")
    
    def _setup_embeddings(self):
        """Cài đặt embedding model"""
        try:
            # Sử dụng sentence-transformers model cho tiếng Việt
            model_name = "keepitreal/vietnamese-sbert"
            self.embeddings = HuggingFaceEmbeddings(
                model_name=model_name,
                model_kwargs={'device': 'cpu'},  # Sử dụng CPU cho compatibility
                encode_kwargs={'normalize_embeddings': True}
            )
            self.logger.info(f"Embeddings model loaded: {model_name}")
        except Exception as e:
            self.logger.warning(f"Failed to load Vietnamese model, using multilingual: {e}")
            # Fallback to multilingual model
            self.embeddings = HuggingFaceEmbeddings(
                model_name="sentence-transformers/all-MiniLM-L6-v2",
                model_kwargs={'device': 'cpu'},
                encode_kwargs={'normalize_embeddings': True}
            )
    
    def _setup_chromadb(self):
        """Cài đặt kết nối ChromaDB"""
        try:
            # Parse host and port
            if ':' in self.chroma_host:
                host, port = self.chroma_host.split(':')
                port = int(port)
            else:
                host = self.chroma_host
                port = 8000
            
            # Initialize ChromaDB client
            self.chroma_client = chromadb.HttpClient(
                host=host,
                port=port,
                settings=Settings(
                    anonymized_telemetry=False,
                    allow_reset=True
                )
            )
            
            # Test connection
            self.chroma_client.heartbeat()
            
            # Get or create collection
            try:
                self.collection = self.chroma_client.get_collection(name=self.collection_name)
                self.logger.info(f"Connected to existing collection: {self.collection_name}")
            except Exception:
                # Create new collection
                self.collection = self.chroma_client.create_collection(
                    name=self.collection_name,
                    metadata={"description": "NHNN macro policies documents"}
                )
                self.logger.info(f"Created new collection: {self.collection_name}")
            
            # Initialize LangChain Chroma vectorstore
            self.vectorstore = Chroma(
                client=self.chroma_client,
                collection_name=self.collection_name,
                embedding_function=self.embeddings
            )
            
            self.logger.info(f"ChromaDB connected successfully at {self.chroma_host}")
            
        except Exception as e:
            self.logger.error(f"Failed to connect to ChromaDB: {e}")
            raise
    
    def _load_processed_files(self):
        """Load danh sách files đã xử lý"""
        processed_file = Path('processed_files.txt')
        if processed_file.exists():
            with open(processed_file, 'r', encoding='utf-8') as f:
                self.processed_files = set(line.strip() for line in f)
            self.logger.info(f"Loaded {len(self.processed_files)} processed files")
    
    def _save_processed_files(self):
        """Lưu danh sách files đã xử lý"""
        with open('processed_files.txt', 'w', encoding='utf-8') as f:
            for file_path in sorted(self.processed_files):
                f.write(f"{file_path}\n")
    
    def _get_file_hash(self, file_path: Path) -> str:
        """Tính hash của file để track changes"""
        hasher = hashlib.md5()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hasher.update(chunk)
        return hasher.hexdigest()
    
    def _load_document(self, file_path: Path) -> List[Document]:
        """Load document từ file path"""
        try:
            file_extension = file_path.suffix.lower()
            
            if file_extension == '.pdf':
                loader = PyPDFLoader(str(file_path))
            elif file_extension in ['.docx', '.doc']:
                loader = Docx2txtLoader(str(file_path))
            elif file_extension == '.txt':
                loader = TextLoader(str(file_path), encoding='utf-8')
            else:
                self.logger.warning(f"Unsupported file format: {file_extension}")
                return []
            
            documents = loader.load()
            self.logger.info(f"Loaded {len(documents)} pages from {file_path.name}")
            return documents
            
        except Exception as e:
            self.logger.error(f"Error loading document {file_path}: {e}")
            return []
    
    def _create_chunk_metadata(self, doc: Document, chunk_id: str, file_path: Path) -> Dict[str, Any]:
        """Tạo metadata cho chunk theo format JSON chuẩn"""
        # Metadata bắt buộc theo yêu cầu
        metadata = {
            "doc_name": file_path.name,
            "upload_time": datetime.now(timezone.utc).isoformat(),
            "chunk_id": chunk_id
        }
        
        # Thêm metadata từ document gốc nếu có
        if hasattr(doc, 'metadata') and doc.metadata:
            # Chỉ thêm các trường không trùng với required fields
            for key, value in doc.metadata.items():
                if key not in metadata and isinstance(value, (str, int, float, bool)):
                    metadata[key] = value
        
        # Thêm thông tin file
        metadata.update({
            "file_path": str(file_path),
            "file_size": file_path.stat().st_size,
            "file_extension": file_path.suffix.lower(),
            "file_hash": self._get_file_hash(file_path),
            "processing_timestamp": datetime.now(timezone.utc).isoformat()
        })
        
        return metadata
    
    def _chunk_documents(self, documents: List[Document], file_path: Path) -> List[Document]:
        """Chia documents thành chunks với metadata"""
        chunks = []
        
        for doc_idx, doc in enumerate(documents):
            # Split document thành chunks
            text_chunks = self.text_splitter.split_text(doc.page_content)
            
            for chunk_idx, chunk_text in enumerate(text_chunks):
                # Tạo unique chunk ID
                chunk_id = f"{file_path.stem}_{doc_idx}_{chunk_idx}_{int(time.time())}"
                
                # Tạo metadata theo format JSON chuẩn
                chunk_metadata = self._create_chunk_metadata(doc, chunk_id, file_path)
                
                # Tạo Document chunk
                chunk_doc = Document(
                    page_content=chunk_text,
                    metadata=chunk_metadata
                )
                chunks.append(chunk_doc)
        
        self.logger.info(f"Created {len(chunks)} chunks from {len(documents)} pages")
        return chunks
    
    def _add_documents_to_vectorstore(self, chunks: List[Document]) -> bool:
        """Thêm chunks vào ChromaDB vectorstore"""
        try:
            if not chunks:
                return True
            
            self.logger.info(f"Adding {len(chunks)} chunks to vectorstore...")
            
            # Process in batches để tránh memory issues
            batch_size = self.batch_size
            for i in tqdm(range(0, len(chunks), batch_size), desc="Processing batches"):
                batch = chunks[i:i + batch_size]
                
                # Extract texts, metadatas, and generate IDs
                texts = [doc.page_content for doc in batch]
                metadatas = []
                ids = []
                
                for doc in batch:
                    # Đảm bảo metadata là JSON serializable
                    metadata = {}
                    for key, value in doc.metadata.items():
                        if isinstance(value, (str, int, float, bool, type(None))):
                            metadata[key] = value
                        else:
                            # Convert complex types to string
                            metadata[key] = str(value)
                    
                    metadatas.append(metadata)
                    ids.append(doc.metadata.get('chunk_id', f'chunk_{i}_{int(time.time())}'))
                
                # Add to vectorstore
                self.vectorstore.add_texts(
                    texts=texts,
                    metadatas=metadatas,
                    ids=ids
                )
                
                time.sleep(0.1)  # Small delay between batches
            
            self.logger.info(f"Successfully added {len(chunks)} chunks to ChromaDB")
            return True
            
        except Exception as e:
            self.logger.error(f"Error adding documents to vectorstore: {e}")
            return False
    
    def process_file(self, file_path: Path) -> bool:
        """Xử lý một file document"""
        try:
            self.logger.info(f"Processing file: {file_path}")
            
            # Check if file was already processed
            file_key = f"{file_path}_{self._get_file_hash(file_path)}"
            if file_key in self.processed_files:
                self.logger.info(f"File already processed: {file_path.name}")
                return True
            
            # Load document
            documents = self._load_document(file_path)
            if not documents:
                self.logger.warning(f"No content loaded from {file_path}")
                return False
            
            # Chunk documents
            chunks = self._chunk_documents(documents, file_path)
            if not chunks:
                self.logger.warning(f"No chunks created from {file_path}")
                return False
            
            # Add to vectorstore
            if self._add_documents_to_vectorstore(chunks):
                # Mark as processed
                self.processed_files.add(file_key)
                self._save_processed_files()
                
                self.logger.info(f"Successfully processed {file_path.name} - {len(chunks)} chunks")
                return True
            else:
                self.logger.error(f"Failed to add chunks to vectorstore for {file_path}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error processing file {file_path}: {e}")
            return False
    
    def scan_and_process_directory(self) -> int:
        """Quét và xử lý tất cả files trong thư mục"""
        self.logger.info(f"Scanning directory: {self.docs_dir}")
        
        # Supported file extensions
        supported_extensions = {'.pdf', '.docx', '.doc', '.txt'}
        
        # Find all supported files
        all_files = []
        for ext in supported_extensions:
            all_files.extend(self.docs_dir.rglob(f'*{ext}'))
        
        if not all_files:
            self.logger.info("No supported documents found in directory")
            return 0
        
        self.logger.info(f"Found {len(all_files)} documents to process")
        
        # Process each file
        processed_count = 0
        for file_path in all_files:
            if self.process_file(file_path):
                processed_count += 1
            time.sleep(1)  # Small delay between files
        
        self.logger.info(f"Processing complete: {processed_count}/{len(all_files)} files processed")
        return processed_count
    
    def get_collection_stats(self) -> Dict[str, Any]:
        """Lấy thống kê collection"""
        try:
            count = self.collection.count()
            
            # Sample một số documents để xem metadata
            if count > 0:
                sample = self.collection.get(limit=min(5, count))
                sample_metadata = sample.get('metadatas', [])
            else:
                sample_metadata = []
            
            stats = {
                'collection_name': self.collection_name,
                'document_count': count,
                'processed_files_count': len(self.processed_files),
                'chroma_host': self.chroma_host,
                'sample_metadata': sample_metadata[:3]  # Show first 3 samples
            }
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Error getting collection stats: {e}")
            return {'error': str(e)}
    
    def search_documents(self, query: str, k: int = 5) -> List[Dict[str, Any]]:
        """Tìm kiếm documents trong collection"""
        try:
            results = self.vectorstore.similarity_search(query, k=k)
            
            search_results = []
            for doc in results:
                result = {
                    'content': doc.page_content[:200] + '...' if len(doc.page_content) > 200 else doc.page_content,
                    'metadata': doc.metadata,
                    'doc_name': doc.metadata.get('doc_name', 'Unknown'),
                    'chunk_id': doc.metadata.get('chunk_id', 'Unknown')
                }
                search_results.append(result)
            
            return search_results
            
        except Exception as e:
            self.logger.error(f"Error searching documents: {e}")
            return []


class DocumentWatcher(FileSystemEventHandler):
    """File system watcher cho tự động xử lý files mới"""
    
    def __init__(self, ingester: NHNNDocumentIngester):
        self.ingester = ingester
        self.logger = logging.getLogger('DocumentWatcher')
        
        # Supported extensions
        self.supported_extensions = {'.pdf', '.docx', '.doc', '.txt'}
    
    def on_created(self, event):
        """Xử lý khi file mới được tạo"""
        if not event.is_directory:
            file_path = Path(event.src_path)
            if file_path.suffix.lower() in self.supported_extensions:
                self.logger.info(f"New document detected: {file_path.name}")
                # Đợi file được ghi xong
                time.sleep(2)
                self.ingester.process_file(file_path)
    
    def on_modified(self, event):
        """Xử lý khi file được chỉnh sửa"""
        if not event.is_directory:
            file_path = Path(event.src_path)
            if file_path.suffix.lower() in self.supported_extensions:
                self.logger.info(f"Document modified: {file_path.name}")
                # Đợi file được ghi xong
                time.sleep(2)
                self.ingester.process_file(file_path)


def main():
    """Main function để chạy NHNN Document Ingester"""
    import argparse
    
    parser = argparse.ArgumentParser(description="NHNN Document Ingester")
    parser.add_argument('--mode', choices=['scan', 'watch', 'stats', 'search'], 
                       default='scan', help='Operation mode')
    parser.add_argument('--query', type=str, help='Search query for search mode')
    parser.add_argument('--limit', type=int, default=5, help='Number of search results')
    
    args = parser.parse_args()
    
    # Initialize ingester
    ingester = NHNNDocumentIngester()
    
    if args.mode == 'scan':
        # Scan and process all documents
        processed = ingester.scan_and_process_directory()
        print(f"Processed {processed} documents")
        
    elif args.mode == 'watch':
        # Watch for new/modified documents
        event_handler = DocumentWatcher(ingester)
        observer = Observer()
        observer.schedule(event_handler, str(ingester.docs_dir), recursive=True)
        
        print(f"Watching directory: {ingester.docs_dir}")
        print("Press Ctrl+C to stop...")
        
        observer.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            observer.stop()
        observer.join()
        
    elif args.mode == 'stats':
        # Show collection statistics
        stats = ingester.get_collection_stats()
        print("Collection Statistics:")
        print(json.dumps(stats, indent=2, ensure_ascii=False))
        
    elif args.mode == 'search':
        # Search documents
        if not args.query:
            print("Error: --query required for search mode")
            return
            
        results = ingester.search_documents(args.query, args.limit)
        print(f"Search results for: '{args.query}'")
        print(json.dumps(results, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()