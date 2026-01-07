import argparse

import uvicorn

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the Decrypto server")
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port to run the server on (default: 8000)",
    )

    args = parser.parse_args()

    uvicorn.run(
        "app:app",
        reload=True,
        host="0.0.0.0",  # if visible from the outside, else 127.0.0.1
        port=args.port,
    )
