from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, ParamSpec, TypeVar
from unittest.mock import Mock

import pytest
from dishka import Provider, Scope, make_async_container, make_container, provide
from dishka.integrations.base import InjectFunc
from faststream import ContextRepo, FastStream
from faststream.nats import NatsBroker, TestNatsBroker

from dishka_faststream import (
    FastStreamProvider,
    FromDishka,
    inject,
    setup_dishka,
    wrap_callback,
)

from .common import (
    APP_DEP_VALUE,
    REQUEST_DEP_VALUE,
    AppDep,
    AppProvider,
    RequestDep,
)

_ParamsP = ParamSpec("_ParamsP")
_ReturnT = TypeVar("_ReturnT")


@asynccontextmanager
async def dishka_app(
    view: Callable[..., Any],
    provider: AppProvider,
    *,
    auto_inject: bool | InjectFunc[_ParamsP, _ReturnT] = False,
) -> AsyncIterator[NatsBroker]:
    broker = NatsBroker()
    sub = broker.subscriber("test")
    sub(inject(view))

    app = FastStream(broker)

    container = make_async_container(provider)
    setup_dishka(container, app=app, auto_inject=auto_inject)

    async with TestNatsBroker(broker) as br:
        assert isinstance(br, NatsBroker)
        yield br

    await container.close()


async def get_with_app(
    a: FromDishka[AppDep],
    mock: FromDishka[Mock],
) -> str:
    mock(a)
    return "passed"


@pytest.mark.asyncio()
async def test_app_dependency(app_provider: AppProvider) -> None:
    async with dishka_app(get_with_app, app_provider) as client:
        msg = await client.request("", "test")
        assert await msg.decode() == "passed"

        app_provider.mock.assert_called_with(APP_DEP_VALUE)
        app_provider.app_released.assert_not_called()
    app_provider.app_released.assert_called()


async def get_with_request(
    a: FromDishka[RequestDep],
    mock: FromDishka[Mock],
) -> str:
    mock(a)
    return "passed"


@pytest.mark.asyncio()
async def test_request_dependency(app_provider: AppProvider) -> None:
    async with dishka_app(get_with_request, app_provider) as client:
        msg = await client.request("", "test")
        assert await msg.decode() == "passed"

        app_provider.mock.assert_called_with(REQUEST_DEP_VALUE)
        app_provider.request_released.assert_called_once()


@pytest.mark.asyncio()
async def test_autoinject_before_subscriber(app_provider: AppProvider) -> None:
    broker = NatsBroker()
    app = FastStream(broker)

    container = make_async_container(app_provider)
    setup_dishka(container, app=app, auto_inject=True)

    sub = broker.subscriber("test")
    sub(get_with_request)

    async with TestNatsBroker(broker) as br:
        assert isinstance(br, NatsBroker)
        msg = await br.request("", "test")
        assert await msg.decode() == "passed"

        app_provider.mock.assert_called_with(REQUEST_DEP_VALUE)
        app_provider.request_released.assert_called_once()

    await container.close()


@pytest.mark.asyncio()
async def test_autoinject_after_subscriber(app_provider: AppProvider) -> None:
    broker = NatsBroker()
    app = FastStream(broker)

    sub = broker.subscriber("test")
    sub(get_with_request)

    container = make_async_container(app_provider)
    setup_dishka(container, app=app, auto_inject=True)

    async with TestNatsBroker(broker) as br:
        assert isinstance(br, NatsBroker)
        msg = await br.request("", "test")
        assert await msg.decode() == "passed"

        app_provider.mock.assert_called_with(REQUEST_DEP_VALUE)
        app_provider.request_released.assert_called_once()

    await container.close()


@pytest.mark.asyncio()
async def test_faststream_with_broker(app_provider: AppProvider) -> None:
    broker = NatsBroker()

    sub = broker.subscriber("test")
    sub(get_with_request)

    container = make_async_container(app_provider)
    setup_dishka(container, broker=broker, auto_inject=True)

    async with TestNatsBroker(broker) as br:
        assert isinstance(br, NatsBroker)
        msg = await br.request("", "test")
        assert await msg.decode() == "passed"

        app_provider.mock.assert_called_with(REQUEST_DEP_VALUE)
        app_provider.request_released.assert_called_once()

    await container.close()


async def handle_for_custom_inject(
    a: FromDishka[AppDep],
    mock: FromDishka[Mock],
) -> str:
    mock(a)
    return "passed"


@pytest.mark.asyncio()
async def test_custom_auto_inject(app_provider: AppProvider) -> None:
    async with dishka_app(
        handle_for_custom_inject,
        app_provider,
        auto_inject=inject,
    ) as client:
        msg = await client.request("", "test")
        assert await msg.decode() == "passed"

        app_provider.mock.assert_called_with(APP_DEP_VALUE)
        app_provider.app_released.assert_not_called()
    app_provider.app_released.assert_called()


@dataclass
class CallbackDependency:
    context: ContextRepo
    request: RequestDep


class CallbackProvider(Provider):
    @provide(scope=Scope.REQUEST)
    def dependency(
        self,
        context: ContextRepo,
        request: RequestDep,
    ) -> CallbackDependency:
        return CallbackDependency(context, request)


@pytest.mark.asyncio()
async def test_async_callback_with_context(app_provider: AppProvider) -> None:
    container = make_async_container(
        app_provider,
        CallbackProvider(),
        FastStreamProvider(),
    )
    context = ContextRepo()
    broker_error = ValueError("broker error")
    received: list[Exception] = []

    async def callback(
        error: Exception,
        dependency: FromDishka[CallbackDependency],
    ) -> None:
        received.append(error)
        assert dependency.context is context
        assert dependency.request == REQUEST_DEP_VALUE
        app_provider.request_released.assert_not_called()

    wrapped = wrap_callback(callback=callback, container=container, context=context)
    try:
        await wrapped(broker_error)
        assert len(received) == 1
        assert received[0] is broker_error
        app_provider.request_released.assert_called_once()
    finally:
        await container.close()


def test_sync_callback_with_context(app_provider: AppProvider) -> None:
    container = make_container(
        app_provider,
        CallbackProvider(),
        FastStreamProvider(),
    )
    context = ContextRepo()
    broker_error = ValueError("broker error")
    received: list[Exception] = []

    def callback(
        error: Exception,
        dependency: FromDishka[CallbackDependency],
    ) -> None:
        received.append(error)
        assert dependency.context is context
        assert dependency.request == REQUEST_DEP_VALUE
        app_provider.request_released.assert_not_called()

    wrapped = wrap_callback(callback=callback, container=container, context=context)
    try:
        wrapped(broker_error)
        assert len(received) == 1
        assert received[0] is broker_error
        app_provider.request_released.assert_called_once()
    finally:
        container.close()
