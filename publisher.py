import asyncio

class publisher:
    def __init__(self):
        self.reader=None
        self.writer=None
    async def run(self):
        while True:
            comando=input("Inserisci l'indirizzo del broker, il topic e il messaggio da pubblicare: ")
            HOST=comando.split()[0]
            topic=comando.split()[1]
            message=comando.split()[2]
            if self.writer=="quit":
                break
            self.reader, self.writer = await asyncio.open_connection(HOST, 8888)
            self.writer.write(f"pub:{topic}:{message}\n".encode())
            await self.writer.drain()
async def main():
    await publisher().run()
asyncio.run(main())           