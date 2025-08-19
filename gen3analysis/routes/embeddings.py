import time
from importlib.metadata import version

from fastapi import APIRouter, Depends, Request
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from starlette import status
from starlette.responses import JSONResponse

from gen3analysis.auth import Auth
from gen3analysis.config import logger

embeddings = APIRouter()


class Embedded_vector(BaseModel):
    embedding_type: str | None = None
    vector: List[float]


@embeddings.post(
    "/cosine/",
    status_code=status.HTTP_200_OK,
    description="Returns the cosine similarity of a given embedding",
    summary="Using cosine similarity to embeddings in vector store find the closest embedding",
    responses={
        status.HTTP_200_OK: {
            "description": "No content",
        },
    },
)
async def cosine_query(
    embedding: Embedded_vector,
) -> JSONResponse:

    try:
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "embedding type": embedding.embedding_type,
                "embedded vector": embedding.vector,
            },
        )

    except ValueError as e:
        logger.error(f"Error while finding similar embeddings: {e}")
        raise HTTPException(status_code=500, detail="Error with supplied embedding")

    except Exception as e:
        logger.error(f"Error while finding similar embeddings: {e}")
        raise HTTPException(status_code=500)


# access_token: Optinal[str] = Cookie(None),
# auth: Auth = Depends(Auth)
