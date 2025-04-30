import chromadb
from langchain_ollama import OllamaEmbeddings
from chromadb.utils.embedding_functions import EmbeddingFunction


class LangchainEmbeddingFunction(EmbeddingFunction):
    """
    Adapter class to make langchain embeddings compatible with ChromaDB
    """

    def __init__(self, model_name):
        self.embedding_model = OllamaEmbeddings(model=model_name)

    def __call__(self, input):
        """
        Generate embeddings for the input texts

        Args:
            input: List of texts to embed

        Returns:
            List of embeddings, one per input text
        """
        embeddings = []
        for text in input:
            embedding = self.embedding_model.embed_query(text)
            embeddings.append(embedding)
        return embeddings


def store_schemas(schema_chunks, config):
    """
    Store schema chunks in ChromaDB with embeddings

    Args:
        schema_chunks: List of dicts with content and metadata
        config: Configuration dictionary
    """
    # Use PersistentClient instead of Client with Settings
    chroma_client = chromadb.PersistentClient(
        path=config["chromadb_path"]
    )

    # Create embedding function with proper interface
    embedding_function = LangchainEmbeddingFunction(config["embedding_model"])

    # Create or get collection
    collection = chroma_client.get_or_create_collection(
        name="schemas",
        embedding_function=embedding_function
    )

    # Prepare data for ChromaDB
    documents = [chunk["content"] for chunk in schema_chunks]
    metadatas = [chunk["metadata"] for chunk in schema_chunks]
    ids = [f"{chunk['metadata']['schema']}_{chunk['metadata']['table']}" for chunk in schema_chunks]

    # Add documents to collection
    collection.add(
        documents=documents,
        metadatas=metadatas,
        ids=ids
    )

    print(f"✅ Stored {len(schema_chunks)} schema chunks in ChromaDB.")
    return collection