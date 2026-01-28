Core API Usage
==============

This guide covers advanced usage of the FLUXNET Shuttle Library's core API, including the plugin system and error handling.

Overview
--------

The FLUXNET Shuttle Library uses a plugin-based architecture where each FLUXNET data hub is implemented as a plugin. The core API provides direct access to these plugins and the orchestrator that coordinates them.

Key Components
~~~~~~~~~~~~~~

- **FluxnetShuttle**: Main orchestrator that coordinates multiple data hub plugins
- **DataHubPlugin**: Abstract base class for data hub-specific implementations
- **PluginRegistry**: Manages plugin registration and instantiation
- **ErrorCollectingIterator**: Async iterator that collects errors while continuing to yield results

Using FluxnetShuttle
--------------------

The ``FluxnetShuttle`` class provides an interface for working with multiple data hub plugins simultaneously.

Basic Usage
~~~~~~~~~~~

.. code-block:: python

    from fluxnet_shuttle.core.shuttle import FluxnetShuttle

    # Create shuttle instance (automatically loads all registered plugins)
    shuttle = FluxnetShuttle()

    # Fetch data from all data hubs asynchronously
    sites = []
    for site in shuttle.get_all_sites():
        sites.append(site)

    # Access results
    print(f"Retrieved {len(sites)} sites")

Downloading Datasets
~~~~~~~~~~~~~~~~~~~~

The ``download_dataset()`` method streams dataset files from a specific data hub. It uses the same orchestrator pattern as ``get_all_sites()`` for consistent error handling.

.. code-block:: python

    from fluxnet_shuttle.core.shuttle import FluxnetShuttle

    # Create shuttle instance
    shuttle = FluxnetShuttle()

    # Download a dataset (sync interface - streams byte chunks)
    # Optional: Include user information for AmeriFlux tracking
    user_info = {
        "ameriflux": {
            "user_name": "Jane Doe",
            "user_email": "jane.doe@example.edu",
            "intended_use": 1,  # 1=synthesis, 2=model, 3=remote_sensing, 4=other_research, 5=education, 6=other
            "description": "Carbon cycle research"
        }
    }

    with open("download.zip", "wb") as file:
        for chunk in shuttle.download_dataset(
            site_id="US-Ha1",
            data_hub="ameriflux",
            download_link="https://amfcdn.lbl.gov/...",
            user_info=user_info,
        ):
            file.write(chunk)

    # Check for errors after download
    # (see Error Handling section below for more details)
    error_summary = shuttle.get_errors()
    if error_summary.total_errors > 0:
        print(f"Download completed with {error_summary.total_errors} errors")

Error Handling
--------------

The library provides comprehensive error handling with Pydantic models for type-safe error reporting.

Programmatic Error Handling
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from fluxnet_shuttle.core.shuttle import FluxnetShuttle

    # Create shuttle instance
    shuttle = FluxnetShuttle()

    # Fetch data from all data hubs
    sites = []
    for site in shuttle.get_all_sites():
        sites.append(site)

    # Get error summary (returns Pydantic ErrorSummary model)
    error_summary = shuttle.get_errors()
    print(f"Total results: {error_summary.total_results}")
    print(f"Total errors: {error_summary.total_errors}")

    # Access detailed error information
    for error in error_summary.errors:
        print(f"Data Hub: {error.data_hub}")
        print(f"Operation: {error.operation}")
        print(f"Error: {error.error}")
        print(f"Timestamp: {error.timestamp}")

    # Error handling also works with downloads
    user_info = {
        "ameriflux": {
            "user_name": "Jane Doe",
            "user_email": "jane.doe@example.edu",
            "intended_use": 1, # 1=synthesis, 2=model, 3=remote_sensing, 4=other_research, 5=education, 6=other
            "description": "Carbon cycle research"
        }
    }

    try:
        with open("download.zip", "wb") as file:
            for chunk in shuttle.download_dataset(
                site_id="US-Ha1",
                data_hub="ameriflux",
                download_link="https://amfcdn.lbl.gov/...",
                user_info=user_info,
            ):
                file.write(chunk)
    except Exception as e:
        print(f"Download failed: {e}")

    # Check error summary after download
    error_summary = shuttle.get_errors()
    if error_summary.total_errors > 0:
        for error in error_summary.errors:
            print(f"Error in {error.operation}: {error.error}")

The `ErrorSummary` model includes:

- `total_errors` (int): Total number of errors encountered
- `total_results` (int): Total number of successful results retrieved
- `errors` (List[PluginErrorDetail]): Detailed error information with data hub, operation, error message, and ISO timestamp

Working with Individual Data Hub Plugins
----------------------------------------

You can also work with individual data hub plugins directly:

.. code-block:: python

    from fluxnet_shuttle.core.registry import registry

    # List all available plugins
    plugin_names = registry.list_plugins()
    print(f"Available plugins: {plugin_names}")  # ['ameriflux', 'icos', 'tern']

    # Create a plugin instance
    ameriflux = registry.create_instance("ameriflux")

    # Use the plugin (sync interface)
    for site in ameriflux.get_sites():
        print(f"AmeriFlux site: {site.site_id}")

Data Hub Plugin Discovery
~~~~~~~~~~~~~~~~~~~~~~~~~

List all available data hub plugins and create instances:

.. code-block:: python

    from fluxnet_shuttle.core.registry import registry

    # Get all registered plugin names
    plugin_names = registry.list_plugins()
    print(f"Available plugins: {plugin_names}")

    # Create plugin instances and use them
    for name in plugin_names:
        plugin = registry.create_instance(name)
        print(f"Plugin: {plugin.display_name}")

Async/Sync Bridge
-----------------

The library provides both async and sync interfaces using decorators. Choose the appropriate interface based on your execution context.


Synchronous Interface
~~~~~~~~~~~~~~~~~~~~~

For normal Python scripts and synchronous contexts, use regular for loops:

.. code-block:: python

    from fluxnet_shuttle.core.shuttle import FluxnetShuttle

    shuttle = FluxnetShuttle()

    # Sync interface - works everywhere
    for site in shuttle.get_all_sites():
        print(f"Site: {site.site_info.site_id}")

    # Downloads also support sync interface
    user_info = {
        "ameriflux": {
            "user_name": "Jane Doe",
            "user_email": "jane.doe@example.edu",
            "intended_use": 4,  # 1=synthesis, 2=model, 3=remote_sensing, 4=other_research, 5=education, 6=other
            "description": "Ecosystem modeling study"
        }
    }

    with open("download.zip", "wb") as file:
        for chunk in shuttle.download_dataset(
            site_id="US-Ha1",
            data_hub="ameriflux",
            download_link="https://amfcdn.lbl.gov/...",
            user_info=user_info,
        ):
            file.write(chunk)

Asynchronous Interface
~~~~~~~~~~~~~~~~~~~~~~

Use the async interface when you're in an async context:

- Inside async functions

.. code-block:: python

    import asyncio
    from fluxnet_shuttle.core.shuttle import FluxnetShuttle

    async def fetch_sites():
        shuttle = FluxnetShuttle()

        # Async interface (preferred for concurrent operations)
        sites = []
        async for site in shuttle.get_all_sites():
            sites.append(site)

        return sites

    async def download_file():
        shuttle = FluxnetShuttle()

        # Downloads also support async interface
        user_info = {
            "ameriflux": {
                "user_name": "Jane Doe",
                "user_email": "jane.doe@example.edu",
                "intended_use": 2,  # 1=synthesis, 2=model, 3=remote_sensing, 4=other_research, 5=education, 6=other
                "description": "Climate model validation"
            }
        }

        with open("download.zip", "wb") as file:
            async for chunk in shuttle.download_dataset(
                site_id="US-Ha1",
                data_hub="ameriflux",
                download_link="https://amfcdn.lbl.gov/...",
                user_info=user_info,
            ):
                file.write(chunk)

    # Run in async context
    sites = asyncio.run(fetch_sites())
    asyncio.run(download_file())

In Jupyter notebooks or async frameworks, you can use async directly:

.. code-block:: python

    # In Jupyter notebook or FastAPI
    shuttle = FluxnetShuttle()

    # Fetch sites
    async for site in shuttle.get_all_sites():
        print(f"Site: {site.site_info.site_id}")

    # Download datasets
    user_info = {
        "ameriflux": {
            "user_name": "Jane Doe",
            "user_email": "jane.doe@example.edu",
            "intended_use": 5,  # 1=synthesis, 2=model, 3=remote_sensing, 4=other_research, 5=education, 6=other
            "description": "Educational workshop on flux data"
        }
    }

    with open("download.zip", "wb") as file:
        async for chunk in shuttle.download_dataset(
            site_id="US-Ha1",
            data_hub="ameriflux",
            download_link="https://amfcdn.lbl.gov/...",
            user_info=user_info,
        ):
            file.write(chunk)

