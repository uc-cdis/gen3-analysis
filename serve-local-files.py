import json
import os

from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
import uvicorn


app = FastAPI()


@app.api_route("/data/{file_path:path}", methods=["GET"])
async def download(file_path: str):
    return {"url": f"http://0.0.0.0:8000/download/{file_path}"}


@app.api_route("/download/{file_path:path}", methods=["GET"])
async def get_file(file_path: str, request: Request):
    # Reading headers
    headers = dict(request.headers)

    # Reading body as bytes, then decode it as string if necessary
    body_bytes = await request.body()
    try:
        body = body_bytes.decode()
    except UnicodeDecodeError:
        body = str(body_bytes)  # In case of binary data

    # Log the request details
    # print(f"Method: {request.method}")
    # print(f"Path: {file_path}")
    # print(f"Headers: {headers}")
    try:
        body = json.loads(body)
    except Exception:
        pass
    # print(f"Body: {body}")

    if not os.path.exists(file_path):
        # file_path = "/" + file_path
        # if not os.path.exists(file_path):
        raise Exception(f"No file {file_path}")

    async def file_iterator():
        with open(file_path, "rb") as f:
            while chunk := f.read(8192):  # Read in 8KB chunks
                yield chunk
        # Clean up the temporary file after streaming
        # os.unlink(file_path)

    return StreamingResponse(file_iterator(), media_type="application/octet-stream")


if __name__ == "__main__":
    uvicorn.run("serve-local-files:app", host="0.0.0.0", port=8000, reload=True)
