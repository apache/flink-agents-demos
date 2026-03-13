################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
#################################################################################
import os
from pathlib import Path
from typing import Any, Sequence

import yaml
from elasticsearch import Elasticsearch
from ollama import Client
from pydantic import BaseModel

# =============================================================================
# Configuration
# =============================================================================

ELASTICSEARCH_URL = "http://localhost:9200"
OLLAMA_HOST = "http://localhost:11434"
OLLAMA_TIMEOUT = 30.0
EMBEDDING_MODEL = os.environ.get("OLLAMA_EMBEDDING_MODEL", "nomic-embed-text:latest")
ELASTICSEARCH_INDEX = "my_documents"


# =============================================================================
# Data Models
# =============================================================================

class SopDocument(BaseModel):
    """SOP document model."""

    id: str
    description: str
    metadata: dict[str, Any]
    content: str


# =============================================================================
# Clients (Singleton Pattern)
# =============================================================================

class ClientManager:
    """Manage singleton instances of external service clients."""

    _elasticsearch_client: Elasticsearch | None = None
    _ollama_client: Client | None = None

    @classmethod
    def get_elasticsearch_client(cls) -> Elasticsearch:
        """Get or create Elasticsearch client."""
        if cls._elasticsearch_client is None:
            cls._elasticsearch_client = Elasticsearch(ELASTICSEARCH_URL)
        return cls._elasticsearch_client

    @classmethod
    def get_ollama_client(cls) -> Client:
        """Get or create Ollama client."""
        if cls._ollama_client is None:
            cls._ollama_client = Client(host=OLLAMA_HOST, timeout=OLLAMA_TIMEOUT)
        return cls._ollama_client


# =============================================================================
# Core Functions
# =============================================================================

def load_all_sops_from_file() -> list[SopDocument]:
    """Load all SOP markdown files with YAML front matter.

    Returns:
        List of SopDocument objects parsed from markdown files.
    """
    # Get the sop directory (project_root/sop)
    current_dir = Path(__file__).parent.parent.parent / "sop"
    sops = []

    for md_file in current_dir.glob("*.md"):
        with md_file.open(encoding="utf-8") as f:
            content = f.read()

        if not content.startswith("---"):
            continue

        parts = content.split("---", 2)
        if len(parts) < 3:
            continue

        meta = yaml.safe_load(parts[1])
        document_id = meta.pop("id", md_file.stem)
        description = meta.pop("description", "")

        sops.append(
            SopDocument(
                id=document_id,
                description=description,
                metadata=meta,
                content=content,
            )
        )

    return sops


def get_embedding(
    text: str | Sequence[str],
    model: str = EMBEDDING_MODEL,
    keep_alive: str | None = None,
    **options: Any,
) -> list[float] | list[list[float]]:
    """Get embedding vector for given text using Ollama.

    Args:
        text: Single text string or sequence of texts to embed.
        model: Ollama model name for embedding.
        keep_alive: How long to keep the model loaded.
        truncate: Whether to truncate input if too long.
        **options: Additional model options.

    Returns:
        Single embedding vector if text is string, otherwise list of vectors.
    """
    ollama_client = ClientManager.get_ollama_client()

    response = ollama_client.embed(
        model=model,
        input=text,
        truncate=True,
        keep_alive=keep_alive,
        options=options if options else None,
    )

    embeddings = [list(embedding) for embedding in response.embeddings]
    return embeddings[0] if isinstance(text, str) else embeddings


def upload_sop_to_elasticsearch(sop: SopDocument, index_name: str = ELASTICSEARCH_INDEX) -> None:
    """Upload a single SOP document to Elasticsearch.

    Args:
        sop: SOP document to upload.
        index_name: Elasticsearch index name.
    """
    elasticsearch_client = ClientManager.get_elasticsearch_client()

    vector = get_embedding(sop.description)

    elasticsearch_client.index(
        index=index_name,
        id=sop.id,
        document={
            "_content": sop.content,
            "_vector": vector,
            "_metadata": sop.metadata,
        },
    )


def upload_all_sops() -> int:
    """Upload all SOP documents to Elasticsearch.

    Returns:
        Number of documents uploaded.
    """
    sops = load_all_sops_from_file()

    for sop in sops:
        upload_sop_to_elasticsearch(sop)
        print(f"Uploaded SOP: {sop.id}")

    return len(sops)


# =============================================================================
# Main Entry Point
# =============================================================================

if __name__ == "__main__":
    uploaded_count = upload_all_sops()
    print(f"Successfully uploaded {uploaded_count} SOP documents to Elasticsearch.")
