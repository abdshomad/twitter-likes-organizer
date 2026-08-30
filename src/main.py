import uvicorn
from src.server.app import app
from src.server.config import HOST, PORT


def run():
    uvicorn.run("src.server.app:app", host=HOST, port=PORT, reload=False)


if __name__ == "__main__":
    run()
