import asyncio
import sys

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
    HOST=sys.argv[1]
    topic=sys.argv[2]
    reader, writer = await asyncio.open_connection(HOST, 8888)
    await Subscriber(reader, writer, topic).run()

try:
    asyncio.run(main())
except KeyboardInterrupt:
    print("Chiusura del subscriber...")
    
