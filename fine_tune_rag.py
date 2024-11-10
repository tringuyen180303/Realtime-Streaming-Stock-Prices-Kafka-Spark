from transformers import RagTokenizer, RagSequenceForGeneration, Trainer, TrainingArguments
from datasets import load_dataset, Dataset
import json

# Load and prepare the dataset
with open("qa_pairs.json") as f:
    qa_data = json.load(f)

dataset = Dataset.from_dict({
    "question": [item["question"] for item in qa_data],
    "answer": [item["answer"] for item in qa_data]
})

# Initialize tokenizer and model
tokenizer = RagTokenizer.from_pretrained("facebook/rag-sequence-nq")
model = RagSequenceForGeneration.from_pretrained("facebook/rag-sequence-nq")

# Tokenize the inputs
def preprocess_function(examples):
    inputs = tokenizer(examples["question"], truncation=True, padding="max_length", max_length=256)
    with tokenizer.as_target_tokenizer():
        labels = tokenizer(examples["answer"], truncation=True, padding="max_length", max_length=256)
    inputs["labels"] = labels["input_ids"]
    return inputs

tokenized_dataset = dataset.map(preprocess_function, batched=True)

# Define training arguments
training_args = TrainingArguments(
    output_dir="./rag-stock-finetuned",
    evaluation_strategy="epoch",
    learning_rate=2e-5,
    per_device_train_batch_size=2,
    per_device_eval_batch_size=2,
    num_train_epochs=3,
    weight_decay=0.01,
    save_total_limit=2,
)

# Initialize Trainer
trainer = Trainer(
    model=model,
    args=training_args,
    train_dataset=tokenized_dataset,
    eval_dataset=tokenized_dataset,  # Ideally, use a separate eval set
)

# Start training
trainer.train()

# Save the fine-tuned model
trainer.save_model("./rag-stock-finetuned")
