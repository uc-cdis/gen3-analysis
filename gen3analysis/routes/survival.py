from typing import List

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import Response
from starlette import status
from starlette.responses import JSONResponse

survival = APIRouter()


@survival.get("/test")
async def get_test(
    request: Request,
) -> JSONResponse:
    response = {"results": {}}

    return JSONResponse(status_code=status.HTTP_200_OK, content=response)


@survival.get("/survival")
async def get_test(
    request: Request,
) -> JSONResponse:
    response = {"results": {}}

    return JSONResponse(status_code=status.HTTP_200_OK, content=response)
