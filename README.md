# Spring AI RAG Template (Gradle)

A compact template that demonstrates how to build a Retrieval-Augmented-Generation (RAG) microservice using Spring AI and Java 21.

## Features
- Uses Spring AI (1.0.1) with OpenAI model starter for embeddings + chat.
- In-memory `SimpleVectorStore` for quick local experiments and templates (no external DB required).
- Simple REST API: upload documents and query with RAG.

## Requirements
- Java 21
- Gradle (recommended) or `./gradlew` wrapper
- An OpenAI API key (or other provider supported by Spring AI). Set it in the environment as `OPENAI_API_KEY`.

## Run
1. Set API key: `export OPENAI_API_KEY=sk-...` (Linux/Mac) or use Windows env vars.
2. Build & run with Gradle wrapper: `./gradlew bootRun`
3. POST text docs:
   `curl -X POST http://localhost:8080/api/docs -H 'Content-Type: application/json' -d '{"text":"Your doc text here"}'`
4. Ask a question:
   `curl "http://localhost:8080/api/ask?q=what+is+the+doc+about"`

## Suggestions for production
- Replace `SimpleVectorStore` with a persistent vector store (Redis/PGVector/Chroma/Qdrant). Spring AI has starters to help.
- Add better error handling, observability, and input sanitization.
- Protect the API with authentication/authorization for private data.
