from abc import ABC, abstractmethod


class MessageHandler(ABC):

    @abstractmethod
    def notify(self, message: bytes) -> bytes:
        pass

