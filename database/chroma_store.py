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

    # First, delete existing schema records
    # Get all unique schema names in the chunks
    unique_schemas = set(chunk['metadata']['schema'] for chunk in schema_chunks)

    # Delete existing records for each schema
    for schema_name in unique_schemas:
        try:
            # Find existing records for this schema
            existing = collection.get(where={"schema": schema_name})
            if existing["ids"] and len(existing["ids"]) > 0:
                print(f"Deleting {len(existing['ids'])} existing chunks for schema: {schema_name}")
                collection.delete(ids=existing["ids"])
        except Exception as e:
            print(f"Warning when trying to delete schema {schema_name}: {e}")

    # Prepare data for ChromaDB
    documents = [chunk["content"] for chunk in schema_chunks]
    metadatas = [chunk["metadata"] for chunk in schema_chunks]

    # Create unique IDs with counter suffix to avoid duplicates
    ids = []
    id_counter = {}

    for chunk in schema_chunks:
        base_id = f"{chunk['metadata']['schema']}_{chunk['metadata']['table']}"

        # Initialize counter for this base ID if it doesn't exist
        if base_id not in id_counter:
            id_counter[base_id] = 0

        # Increment counter for this base ID
        id_counter[base_id] += 1

        # Create a unique ID with counter
        unique_id = f"{base_id}_{id_counter[base_id]}"
        ids.append(unique_id)

    # Add documents to collection
    collection.add(
        documents=documents,
        metadatas=metadatas,
        ids=ids
    )

    print(f"✅ Stored {len(schema_chunks)} schema chunks in ChromaDB.")
    return collection