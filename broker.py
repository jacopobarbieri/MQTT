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
            if self.pos=="pub":
                self.message=await self.reader.readline()
                self.topic=await self.reader.readuntil(":").decode().strip()
                for client in self.__class__.clients:
                    if self.__class__.clients[client][2] == self.topic and self.__class__.clients[client][3] == "sub":
                        self.write(self.message)
                self.writer.write
            if self.pos=="sub":
                self.topic=await self.reader.readline().decode().strip()

            self.__class__.clients[self.topic]=[self.reader,self.writer,self.topic,self.pos]
async def main():
    # start_server apre la porta e chiama Broker per ogni nuovo client
    server = await asyncio.start_server(Broker.run, "", 8888)
    print("Server in ascolto sulla porta 8888")
    async with server:
        await server.serve_forever()


asyncio.run(main())