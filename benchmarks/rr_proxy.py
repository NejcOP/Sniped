import argparse
import asyncio
from urllib.parse import urljoin

from aiohttp import ClientSession, web


class RoundRobinProxy:
    def __init__(self, upstreams: list[str]) -> None:
        clean = [u.rstrip("/") for u in upstreams if u.strip()]
        if not clean:
            raise ValueError("At least one upstream is required")
        self._upstreams = clean
        self._index = 0
        self._lock = asyncio.Lock()

    async def _next_upstream(self) -> str:
        async with self._lock:
            upstream = self._upstreams[self._index]
            self._index = (self._index + 1) % len(self._upstreams)
            return upstream

    async def handle(self, request: web.Request) -> web.Response:
        upstream = await self._next_upstream()
        upstream_url = urljoin(upstream + "/", request.rel_url.path.lstrip("/"))
        if request.rel_url.query_string:
            upstream_url = f"{upstream_url}?{request.rel_url.query_string}"

        skip_headers = {"host", "content-length"}
        forwarded_headers = {k: v for k, v in request.headers.items() if k.lower() not in skip_headers}
        body = await request.read()

        async with ClientSession() as session:
            async with session.request(
                request.method,
                upstream_url,
                headers=forwarded_headers,
                data=body,
                allow_redirects=False,
                timeout=60,
            ) as resp:
                payload = await resp.read()
                response_headers = {
                    k: v
                    for k, v in resp.headers.items()
                    if k.lower() not in {"transfer-encoding", "content-encoding", "content-length"}
                }
                return web.Response(status=resp.status, body=payload, headers=response_headers)


def build_app(upstreams: list[str]) -> web.Application:
    proxy = RoundRobinProxy(upstreams)
    app = web.Application()
    app.router.add_route("*", "/{path:.*}", proxy.handle)
    return app


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Round-robin reverse proxy for local load-balance simulation")
    parser.add_argument("--listen-host", default="127.0.0.1")
    parser.add_argument("--listen-port", type=int, default=8000)
    parser.add_argument("--upstreams", required=True, help="Comma-separated upstream URLs")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    upstreams = [u.strip() for u in args.upstreams.split(",") if u.strip()]
    app = build_app(upstreams)
    web.run_app(app, host=args.listen_host, port=args.listen_port, access_log=None)


if __name__ == "__main__":
    main()
