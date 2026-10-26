import asyncio

class Broker:
    clients=[]
    def __init__(self,reader,writer):
        self.reader=reader
        self.writer=writer
        self.pos=None
        self.topic=None
        self.message=None
        self.__class__.clients.append(self)
    async def run(self):
        addr=self.writer.get_extra_info("peername")[0]
        print(f"nuovo client connesso con ip @{addr}")
        while True:
            try:
                self.pos=await self.reader.readuntil(b":")
                self.pos=self.pos.decode().strip(":")
            
                if self.pos=="pub":
                    self.topic=await self.reader.readuntil(b":")
                    self.message=await self.reader.readline()
                    self.topic=self.topic.decode().strip(":")
                    for client in self.__class__.clients:
                        if client.topic == self.topic and client.pos == "sub":
                            client.writer.write(f"{self.message.decode()}".encode())
                            await client.writer.drain()
                if self.pos=="sub":
                    self.topic=await self.reader.readline()
                    self.topic=self.topic.decode().strip("\n")
            except(asyncio.IncompleteReadError, ConnectionResetError):
                print(f"client @{addr} disconnesso")
                self.__class__.clients.remove(self)
                break
async def handle(reader, writer):
    broker = Broker(reader, writer)
    await broker.run()
async def main():
    # start_server apre la porta e chiama Broker per ogni nuovo client
    server = await asyncio.start_server(handle, "", 8888)
    print("Server in ascolto sulla porta 8888")
    async with server:
        await server.serve_forever()


asyncio.run(main())