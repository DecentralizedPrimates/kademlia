import random
import asyncio
import logging

from rpcudp.protocol import RPCProtocol

from kademlia.node import Node
from kademlia.routing import RoutingTable
from kademlia.utils import digest
from kademlia.handlers import MessageHandler

log = logging.getLogger(__name__)  # pylint: disable=invalid-name


class KademliaProtocol(RPCProtocol):
    def __init__(self, source_node, ksize):
        RPCProtocol.__init__(self)
        self.router = RoutingTable(self, ksize, source_node)
        self.message_handler = None
        self.source_node = source_node

    def subscribe(self, message_handler: MessageHandler):
        self.message_handler = message_handler

    def get_refresh_ids(self):
        """
        Get ids to search for to keep old buckets up to date.
        """
        ids = []
        for bucket in self.router.lonely_buckets():
            rid = random.randint(*bucket.range).to_bytes(20, byteorder='big')
            ids.append(rid)
        return ids

    def rpc_stun(self, sender):  # pylint: disable=no-self-use
        return sender

    def rpc_ping(self, sender, nodeid):
        source = Node(nodeid, sender[0], sender[1])
        self.welcome_if_new(source)
        return self.source_node.id

    def rpc_query(self, sender, nodeid, value: bytes):
        source = Node(nodeid, sender[0], sender[1])
        self.welcome_if_new(source)
        return self.message_handler.notify(value)

    def rpc_find_node(self, sender, nodeid, key):
        log.info("finding neighbors of %i in local table",
                 int(nodeid.hex(), 16))
        source = Node(nodeid, sender[0], sender[1])
        self.welcome_if_new(source)
        node = Node(key)
        neighbors = self.router.find_neighbors(node, exclude=source)
        return list(map(tuple, neighbors))

    async def call_find_node(self, node_to_ask, node_to_find):
        address = (node_to_ask.ip, node_to_ask.port)
        result = await self.find_node(address, self.source_node.id,
                                      node_to_find.id)
        return self.handle_call_response(result, node_to_ask)

    async def call_find_value(self, node_to_ask, node_to_find):
        address = (node_to_ask.ip, node_to_ask.port)
        result = await self.find_value(address, self.source_node.id,
                                       node_to_find.id)
        return self.handle_call_response(result, node_to_ask)

    async def call_ping(self, node_to_ask):
        address = (node_to_ask.ip, node_to_ask.port)
        result = await self.ping(address, self.source_node.id)
        return self.handle_call_response(result, node_to_ask)

    async def call_query(self, node_to_ask, node: Node):
        address = (node_to_ask.ip, node_to_ask.port)
        result = await self.query(address, self.source_node.id, node.payload)
        return self.handle_call_response(result, node_to_ask)

    def welcome_if_new(self, node):
        """
        Given a new node, send it all the keys/values it should be storing,
        then add it to the routing table.

        @param node: A new node that just joined (or that we just found out
        about).

        """
        if not self.router.is_new_node(node):
            return

        log.info("never seen %s before, adding to router", node)
        self.router.add_contact(node)

    def handle_call_response(self, result, node):
        """
        If we get a response, add the node to the routing table.  If
        we get no response, make sure it's removed from the routing table.
        """
        if not result[0]:
            log.warning("no response from %s, removing from router", node)
            self.router.remove_contact(node)
            return result

        log.info("got successful response from %s", node)
        self.welcome_if_new(node)
        return result
