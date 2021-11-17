import logging
import asyncio
import sys

from kademlia.network import Server
from examples.handler_example import TestMessageHandler

# if len(sys.argv) != 4:
#     print("Usage: python query.py <bootstrap node> <bootstrap port> <key>")
#     sys.exit(1)

handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
log = logging.getLogger('kademlia')
log.addHandler(handler)
log.setLevel(logging.DEBUG)


async def run():
    server = Server()
    await server.listen(8470, TestMessageHandler())
    bootstrap_node = ("localhost", 8469)
    await server.bootstrap([bootstrap_node])

    result = await server.query("aa".encode())
    print("Get result:", result)
    server.stop()

asyncio.run(run())
