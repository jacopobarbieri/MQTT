import asyncio

class Broker:
    clients={}
    def __init__(self,reader,writer):
        self.reader=reader
        self.writer=writer
        self.pos=None
        self.topic=None
        self.message=None
    async def run(self):
        addr=self.writer.get_extra_info("peername")[0]
        print(f"nuovo client connesso con ip @{addr}")
        while True:
            self.pos=await self.reader.readuntil(":").decode().strip()
            self.topic=await self.reader.readuntil(":").decode().strip()
            if self.pos=="pub":