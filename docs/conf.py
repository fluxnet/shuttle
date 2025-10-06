# Sphinx configuration for FLUXNET Shuttle Library
project = "FLUXNET Shuttle Library"
copyright = "2025, AMF-FLX"
author = "AMF-FLX"
release = "0.1.0"

# Extensions
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.viewcode",
    "sphinx.ext.napoleon",
    "sphinx_autodoc_typehints",
]

# Autosummary settings
autosummary_generate = True
autosummary_imported_members = True

# Autodoc settings
autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "show-inheritance": True,
}

# Napoleon settings for Google/NumPy style docstrings
napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = False
napoleon_include_private_with_doc = False

# General configuration
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]
html_theme = "sphinx_rtd_theme"

# The suffix(es) of source filenames
source_suffix = ".rst"

# The master toctree document
master_doc = "index"

# Add type hints to descriptions
autodoc_typehints = "description"
