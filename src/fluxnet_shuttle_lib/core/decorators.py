"""
Decorators module
=================

:module:: fluxnet_shuttle_lib.core.decorators
:synopsis: Decorators for FLUXNET Shuttle Library
:moduleauthor: Valerie Hendrix <vchendrix@lbl.gov>
:platform: Unix, Windows
:created: 2025-10-09

.. currentmodule:: fluxnet_shuttle_lib.core.decorators
.. autosummary::
    :toctree: generated/
    async_to_sync

This module provides decorators for converting between sync and async operations.
"""

import asyncio
import functools
from typing import TypeVar

T = TypeVar("T")


def async_to_sync(func):
    """
    Decorator: If there is no event loop, run synchronously
    """

    # check if the function is a coroutine function
    if not asyncio.iscoroutinefunction(func):
        raise TypeError("The async_to_sync decorator can only be applied to async functions.")

    @functools.wraps(func)
    def function_wrapper(*args, **kwargs):

        loop = asyncio.get_event_loop()
        if loop.is_running():
            # If there is already a running event loop, just call the async function directly
            # This cannot be converted to sync in this case
            return func(*args, **kwargs)
        else:
            # No running event loop, so we can create one and run the async function synchronously
            # Create a new event loop for this sync call
            # Get the async function
            f = func(*args, **kwargs)
            loop = asyncio.get_event_loop()
            task = loop.create_task(f)
            loop.run_until_complete(task)
            return task.result()

    return function_wrapper


def async_to_sync_generator(func):
    """
    Decorator that enables both async and sync usage of async generator methods.

    This decorator modifies the method so that:
    - When called with `async for`, it works as an async generator
    - When called with regular `for` or `list()`, it works as a sync iterable

    The detection is based on whether the caller tries to use the result
    as an async iterator (__aiter__) or sync iterator (__iter__).
    """

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        """Wrapper that returns a hybrid iterator object."""
        return _HybridAsyncSyncIterator(func, self, *args, **kwargs)

    # Store reference to original function for direct access
    wrapper._original_async_func = func

    return wrapper


class _HybridAsyncSyncIterator:
    """
    A hybrid iterator that can work both as async and sync iterator.
    """

    def __init__(self, async_func, *args, **kwargs):
        self._async_func = async_func
        self._args = args
        self._kwargs = kwargs
        self._async_gen = None
        self._sync_gen = None

    def __aiter__(self):
        """Return self for async iteration."""
        return self

    async def __anext__(self):
        """Async next method."""
        if self._async_gen is None:
            # Call the original async function directly to get the actual async generator
            self._async_gen = self._async_func(*self._args, **self._kwargs)
        return await self._async_gen.__anext__()

    def __iter__(self):
        """Return iterator for sync iteration."""
        if self._sync_gen is None:
            self._sync_gen = _sync_generator_wrapper(self._async_func, *self._args, **self._kwargs)
        return self._sync_gen

    def __next__(self):
        """Sync next method."""
        # Delegate to the sync generator's next method, No use cases for coverage yet.
        return next(self.__iter__())  # pragma: no cover


def _sync_generator_wrapper(async_func, *args, **kwargs):
    """Convert async generator to sync generator."""
    # Create a new event loop for this sync call
    loop = asyncio.get_event_loop()

    try:

        # Ensure no event loop is running
        assert not loop.is_running(), "Event loop is already running, cannot convert to sync generator"

        # Get the async generator
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        async_gen = async_func(*args, **kwargs)
        # Convert to sync generator
        while True:
            try:
                # Get next item from async generator
                item = loop.run_until_complete(async_gen.__anext__())
                yield item
            except StopAsyncIteration:
                break
    finally:
        # Clean up
        if "async_gen" in locals():
            try:
                loop.run_until_complete(async_gen.aclose())
            finally:
                loop.close()
