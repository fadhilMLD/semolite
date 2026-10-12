import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

from sentence_transformers import SentenceTransformer

HOST = "127.0.0.1"
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8765

model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def reply(self, code, payload):
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length)
        if self.path != "/embed":
            return self.reply(404, {"error": "use POST /embed"})
        try:
            text = json.loads(raw)["text"]
            if not isinstance(text, str):
                raise ValueError("text must be a string")
            vector = model.encode(text, normalize_embeddings=True)
        except Exception as exc:
            return self.reply(400, {"error": str(exc)})
        self.reply(200, {"embedding": vector.tolist()})

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    print(f"embedding server ready on http://{HOST}:{PORT}/embed", flush=True)
    HTTPServer((HOST, PORT), Handler).serve_forever()