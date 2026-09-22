"""Type-level checks: mypy reads this module in the lint job, pytest does not collect it.

Every FastStream broker binds the third ``BrokerUsecase`` parameter to its own
config class, so ``setup_dishka`` must accept any config, not the default one.
"""

from dishka import AsyncContainer
from faststream import FastStream
from faststream.confluent import KafkaBroker as ConfluentBroker
from faststream.kafka import KafkaBroker
from faststream.nats import NatsBroker
from faststream.rabbit import RabbitBroker
from faststream.redis import RedisBroker

from dishka_faststream import setup_dishka


def check_setup_accepts_every_broker(container: AsyncContainer) -> None:
    setup_dishka(container, broker=RabbitBroker())
    setup_dishka(container, broker=KafkaBroker())
    setup_dishka(container, broker=ConfluentBroker())
    setup_dishka(container, broker=NatsBroker())
    setup_dishka(container, broker=RedisBroker())


def check_setup_accepts_every_app(container: AsyncContainer) -> None:
    setup_dishka(container, app=FastStream(RabbitBroker()))
    setup_dishka(container, app=FastStream(KafkaBroker()))
    setup_dishka(container, app=FastStream(NatsBroker()))
