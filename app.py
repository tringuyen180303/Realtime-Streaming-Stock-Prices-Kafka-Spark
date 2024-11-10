from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from transformers import RagTokenizer, RagSequenceForGeneration
from sentence_transformers import SentenceTransformer
from elasticsearch import Elasticsearch
import torch
import numpy as np

app = FastAPI()

# Define request body
class Query(BaseModel):
    question: str

# Initialize Elasticsearch client
es = Elasticsearch("http://elasticsearch:9200")

# Initialize tokenizer and retriever
tokenizer = RagTokenizer.from_pretrained("facebook/rag-sequence-nq")

# Custom retriever class
class ElasticsearchRetriever:
    def __init__(self, es_client, index, embedding_model, dims=384):
        self.es = es_client
        self.index = index
        self.embedding_model = embedding_model
        self.dims = dims

    def retrieve(self, question, top_k=5):
        # Generate embedding for the question
        query_embedding = self.embedding_model.encode([question], convert_to_numpy=True)[0]

        # Normalize if using cosine similarity
        norm = np.linalg.norm(query_embedding)
        if norm != 0:
            query_embedding = query_embedding / norm

        # Perform vector search in Elasticsearch
        response = self.es.search(
            index=self.index,
            size=top_k,
            body={
                "query": {
                    "script_score": {
                        "query": { "match_all": {} },
                        "script": {
                            "source": "cosineSimilarity(params.query_vector, 'embedding') + 1.0",
                            "params": { "query_vector": query_embedding.tolist() }
                        }
                    }
                }
            }
        )

        # Extract the retrieved documents
        retrieved_docs = []
        for hit in response['hits']['hits']:
            retrieved_docs.append(hit['_source'])

        return retrieved_docs

# Initialize embedding model
embedding_model = SentenceTransformer('all-MiniLM-L6-v2')

# Initialize retriever
retriever = ElasticsearchRetriever(
    es_client=es,
    index="stock_prices",
    embedding_model=embedding_model,
    dims=384
)

# Initialize RAG model
model = RagSequenceForGeneration.from_pretrained("./rag-stock-finetuned")  # Path to fine-tuned model
model.eval()

@app.post("/query")
async def query_rag(query: Query):
    question = query.question

    # Retrieve relevant documents
    retrieved_docs = retriever.retrieve(question, top_k=5)

    # Prepare inputs for RAG
    inputs = tokenizer(question, return_tensors="pt")

    # If no documents are retrieved, handle accordingly
    if not retrieved_docs:
        raise HTTPException(status_code=404, detail="No relevant documents found.")

    # Convert retrieved documents to strings
    docs = [f"Timestamp: {doc['timestamp']} Symbol: {doc['symbol']} Open: {doc['open']} High: {doc['high']} Low: {doc['low']} Close: {doc['close']} Volume: {doc['volume']}" for doc in retrieved_docs]

    # Encode documents
    inputs["retrieved_doc_embeds"] = torch.tensor([embedding_model.encode(doc) for doc in docs])
    inputs["doc_ids"] = [f"{doc['symbol']}_{doc['timestamp']}" for doc in retrieved_docs]
    inputs["labels"] = None  # No labels in inference

    # Generate answer
    with torch.no_grad():
        generated_ids = model.generate(
            input_ids=inputs["input_ids"],
            attention_mask=inputs["attention_mask"],
            retrieved_doc_embeds=inputs["retrieved_doc_embeds"]
        )
    answer = tokenizer.batch_decode(generated_ids, skip_special_tokens=True)[0]

    return {"answer": answer}
