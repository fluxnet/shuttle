import importlib.metadata

try:
    __version__ = importlib.metadata.version("fluxnet-shuttle-lib")
    if "-" in __version__:
        __release__ = __version__
        __version__ = __version__.split("-")[0]
    else:
        __release__ = __version__
except importlib.metadata.PackageNotFoundError:
    __version__ = "unknown"
    __release__ = "unknown"
