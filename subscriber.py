import asyncio
class Subscriber:
    def __init__(self,reader,writer,topic):
        self.reader=reader
        self.writer=writer
        self.topic=topic
    async def run(self):
        self.writer.write(f"sub:{self.topic}\n".encode())
        await self.writer.drain()
        while True:
            message=await self.reader.readline()
            if not message:
                break
            print(f"Messaggio ricevuto: {message.decode()}")
async def main():
    comando=input("Inserisci il broker e il topic a cui vuoi iscriverti: ")
    HOST=comando.split()[0]
    topic=comando.split()[1]
    reader, writer = await asyncio.open_connection(HOST, 8888)
    await Subscriber(reader, writer, topic).run()


asyncio.run(main())
