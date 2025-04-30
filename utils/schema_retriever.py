# utils/schema_retriever.py
from chromadb import PersistentClient
from langchain_ollama import OllamaEmbeddings


def retrieve_similar(schema_ddl, schema_name, top_k=5, config=None):
    """
    Retrieve similar schemas from ChromaDB

    Args:
        schema_ddl: DDL statement to compare
        schema_name: Name of the schema
        top_k: Number of results to retrieve
        config: Configuration dictionary

    Returns:
        list: List of similar schemas with content and metadata
    """
    embedding = OllamaEmbeddings(model=config["embedding_model"])
    client = PersistentClient(path=config["chromadb_path"])
    collection = client.get_collection("schemas")

    query_vector = embedding.embed_query(schema_ddl)
    results = collection.query(query_embeddings=[query_vector], n_results=top_k)

    similar_schemas = []
    for i in range(len(results["documents"][0])):
        # Skip if the retrieved schema is the same as the query schema
        if results["metadatas"][0][i]["schema"] == schema_name:
            continue

        similar_schemas.append({
            "content": results["documents"][0][i],
            "metadata": results["metadatas"][0][i]
        })

    return similar_schemas