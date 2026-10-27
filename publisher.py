import asyncio
import sys

class publisher:
    def __init__(self):
        self.reader=None
        self.writer=None
    async def run(self):
        HOST=sys.argv[1]
        topic=sys.argv[2]
        message="_".join(sys.argv[3:])
        self.reader, self.writer = await asyncio.open_connection(HOST, 8888)
        self.writer.write(f"pub:{topic}:{message}\n".encode())
        await self.writer.drain()
        self.writer.close()
async def main():
    await publisher().run()
asyncio.run(main())           