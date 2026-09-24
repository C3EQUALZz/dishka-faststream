from faststream import ContextRepo, FastStream
from faststream.nats import NatsBroker, NatsMessage
from nats.aio.client import ErrorCallback

from dishka import AsyncContainer, Provider, Scope, make_async_container, provide
from dishka_faststream import (
    FastStreamProvider,
    FromDishka,
    setup_dishka,
)


class A:
    def __init__(self) -> None:
        pass


class B:
    def __init__(self, a: A) -> None:
        self.a = a


class ErrorHandler:
    def __init__(self, b: B) -> None:
        self.b = b

    async def handle(self, error: Exception) -> None:
        print(f"Broker error: {error!r}; dependency: {self.b!r}")


class MyProvider(Provider):
    @provide(scope=Scope.APP)
    def get_a(self) -> A:
        return A()

    @provide(scope=Scope.REQUEST)
    def get_b(self, a: A) -> B:
        return B(a)

    @provide(scope=Scope.REQUEST)
    def get_error_handler(self, b: B) -> ErrorHandler:
        return ErrorHandler(b)


def make_error_cb(container: AsyncContainer) -> ErrorCallback:
    async def callback(error: Exception) -> None:
        async with container() as ctx:
            handler = await ctx.get(ErrorHandler)
            await handler.handle(error)

    return callback


provider = MyProvider()
container = make_async_container(provider, FastStreamProvider())

broker = NatsBroker(error_cb=make_error_cb(container))
app = FastStream(broker)
setup_dishka(container, app, auto_inject=True)


@broker.subscriber("test")
async def handler(
    msg: str,
    a: FromDishka[A],
    b: FromDishka[B],
    raw_message: FromDishka[NatsMessage],
    faststream_context: FromDishka[ContextRepo],
) -> None:
    print(msg, a, b)


@app.after_startup
async def t() -> None:
    await broker.publish("test", "test")
