from transformers import RagTokenizer, RagRetriever, RagSequenceForGeneration
from sentence_transformers import SentenceTransformer
from elasticsearch import Elasticsearch
import torch

es = Elasticsearch("http://localhost:9200")

tokenizer = RagTokenizer.from_pretrained("facebook/rag-sequence-nq")

class ElasticsearchRetriever(RagRetriever):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.es = Elasticsearch("http://localhost:9200")
        self.index = "stock_prices"
        self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')

    def retrieve(self, question, top_k=5):
        # Generate embedding for the question
        query_embedding = self.embedding_model.encode([question], convert_to_numpy=True)[0]

        # Normalize the embedding if required (for cosine similarity)
        # Cosine similarity is equivalent to inner product if vectors are normalized
        norm = np.linalg.norm(query_embedding)
        if norm != 0:
            query_embedding = query_embedding / norm

        # Perform a vector search in Elasticsearch using cosine similarity
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

# Initialize the retriever
retriever = ElasticsearchRetriever(
    tokenizer=tokenizer,
    es=es,
    index="stock_prices"
)

# Initialize the RAG model
model = RagSequenceForGeneration.from_pretrained("facebook/rag-sequence-nq", retriever=retriever)
model.eval()

# Function to generate answers
def generate_answer(question):
    inputs = tokenizer(question, return_tensors="pt")
    with torch.no_grad():
        generated_ids = model.generate(**inputs)
    answer = tokenizer.batch_decode(generated_ids, skip_special_tokens=True)[0]
    return answer

if __name__ == "__main__":
    while True:
        user_input = input("Enter your question (or 'exit' to quit): ")
        if user_input.lower() == 'exit':
            break
        answer = generate_answer(user_input)
        print(f"Answer: {answer}\n")