from kademlia.handlers import MessageHandler


class TestMessageHandler(MessageHandler):
    def notify(self, message: bytes) -> bytes:
        print(message.decode("utf-8"))
        return "response".encode()

