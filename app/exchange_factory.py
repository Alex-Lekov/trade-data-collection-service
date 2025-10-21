"""Utility helpers to instantiate cryptofeed exchanges by config name."""

from __future__ import annotations

from typing import Any, Dict, Type

from cryptofeed.exchanges import Binance, BinanceFutures


ExchangeClass = Type[Any]


_EXCHANGE_REGISTRY: Dict[str, ExchangeClass] = {
    'binance': Binance,
    'binance_spot': Binance,
    'binance_futures': BinanceFutures,
    'binance-perp': BinanceFutures,
}


def get_exchange_class(name: str) -> ExchangeClass:
    """Return the cryptofeed exchange class configured by ``name``."""

    if not name:
        name = 'binance'
    key = name.strip().lower()
    try:
        return _EXCHANGE_REGISTRY[key]
    except KeyError as exc:
        supported = ', '.join(sorted(_EXCHANGE_REGISTRY.keys()))
        raise ValueError(f'Unsupported exchange "{name}". Supported: {supported}') from exc


def create_exchange(name: str, **kwargs):
    """Instantiate the configured exchange class."""

    exchange_cls = get_exchange_class(name)
    return exchange_cls(**kwargs)
