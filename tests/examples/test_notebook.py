"""
Notebook testing utilities for validating example notebooks.

This module provides utilities to execute and test Jupyter notebooks
programmatically to ensure examples remain functional.
"""

import json
import os
import subprocess
import sys
import tempfile
from typing import Any, Dict, List

import pytest

# Mark all tests in this file as examples tests
pytestmark = pytest.mark.examples


class NotebookTester:
    """Utility class for testing Jupyter notebooks."""

    def __init__(self, notebook_path: str):
        """
        Initialize notebook tester.

        :param notebook_path: Path to the notebook file
        """
        self.notebook_path = notebook_path
        self.notebook_data = None
        self._load_notebook()

    def _load_notebook(self):
        """Load notebook JSON data."""
        with open(self.notebook_path, "r", encoding="utf-8") as f:
            self.notebook_data = json.load(f)

    def get_code_cells(self) -> List[Dict[str, Any]]:
        """Get all code cells from the notebook."""
        return [cell for cell in self.notebook_data["cells"] if cell["cell_type"] == "code"]

    def get_cell_source(self, cell_index: int) -> str:
        """Get source code from a specific cell."""
        cells = self.get_code_cells()
        if cell_index >= len(cells):
            raise IndexError(f"Cell index {cell_index} out of range")
        return "".join(cells[cell_index]["source"])

    def extract_imports(self) -> List[str]:
        """Extract all import statements from the notebook."""
        imports = []
        for cell in self.get_code_cells():
            source = "".join(cell["source"])
            lines = source.split("\n")
            for line in lines:
                line = line.strip()
                if line.startswith("import ") or line.startswith("from "):
                    # Skip commented imports
                    if not line.startswith("#"):
                        imports.append(line)
        return imports

    def validate_imports(self) -> Dict[str, bool]:
        """
        Validate that all imports in the notebook can be imported.

        :return: Dictionary mapping import statements to success status
        """
        import_results = {}
        imports = self.extract_imports()

        for import_stmt in imports:
            try:
                # Execute the import statement
                exec(import_stmt)
                import_results[import_stmt] = True
            except ImportError:
                import_results[import_stmt] = False
            except Exception:
                # Other errors (syntax, etc.)
                import_results[import_stmt] = False

        return import_results

    def execute_notebook(self, timeout: int = 300, kernel: str = "python3") -> tuple[bool, str]:
        """
        Execute the entire notebook using nbconvert.

        :param timeout: Timeout in seconds for notebook execution
        :param kernel: Kernel to use for execution
        :return: Tuple of (success, error_message)
        """
        try:
            with tempfile.NamedTemporaryFile(suffix=".ipynb", delete=False) as temp_nb:
                temp_path = temp_nb.name

            # Execute notebook
            cmd = [
                sys.executable,
                "-m",
                "jupyter",
                "nbconvert",
                "--to",
                "notebook",
                "--execute",
                "--ExecutePreprocessor.timeout={}".format(timeout),
                "--ExecutePreprocessor.kernel_name={}".format(kernel),
                "--output",
                temp_path,
                self.notebook_path,
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout + 60)

            # Clean up
            if os.path.exists(temp_path):
                os.unlink(temp_path)

            if result.returncode == 0:
                return True, "Notebook executed successfully"
            else:
                error_msg = f"Notebook execution failed (return code {result.returncode})\n"
                error_msg += f"STDOUT:\n{result.stdout}\n"
                error_msg += f"STDERR:\n{result.stderr}"
                return False, error_msg

        except subprocess.TimeoutExpired:
            return False, f"Notebook execution timed out after {timeout} seconds"
        except Exception as e:
            return False, f"Exception during notebook execution: {str(e)}"

    def check_required_packages(self) -> Dict[str, bool]:
        """
        Check if required packages are available.

        :return: Dictionary mapping package names to availability
        """
        packages = {
            "pandas": False,
            "matplotlib": False,
            "fluxnet_shuttle_lib": False,
            "zipfile": False,  # Built-in module
            "logging": False,  # Built-in module
        }

        for package in packages:
            try:
                __import__(package)
                packages[package] = True
            except ImportError:
                packages[package] = False

        return packages


@pytest.fixture
def notebook_tester():
    """Pytest fixture for notebook tester."""
    # Get path relative to this test file
    test_dir = os.path.dirname(__file__)
    notebook_path = os.path.join(test_dir, "..", "..", "examples", "fluxnet_shuttle_example.ipynb")
    notebook_path = os.path.abspath(notebook_path)

    if os.path.exists(notebook_path):
        return NotebookTester(notebook_path)
    else:
        pytest.skip(f"Notebook not found: {notebook_path}")


class TestExampleNotebook:
    """Test the example notebook functionality."""

    def test_notebook_exists(self):
        """Test that the example notebook exists."""
        test_dir = os.path.dirname(__file__)
        notebook_path = os.path.join(test_dir, "..", "..", "examples", "fluxnet_shuttle_example.ipynb")
        notebook_path = os.path.abspath(notebook_path)
        assert os.path.exists(notebook_path), f"Notebook not found: {notebook_path}"

    def test_notebook_structure(self, notebook_tester):
        """Test the notebook has expected structure."""
        if notebook_tester is None:
            pytest.skip("Notebook tester not available")

        # Check that notebook has cells
        code_cells = notebook_tester.get_code_cells()
        assert len(code_cells) > 0, "Notebook should have code cells"

        # Check that notebook has markdown cells
        markdown_cells = [cell for cell in notebook_tester.notebook_data["cells"] if cell["cell_type"] == "markdown"]
        assert len(markdown_cells) > 0, "Notebook should have markdown cells"

    def test_required_imports(self, notebook_tester):
        """Test that required imports are present."""
        if notebook_tester is None:
            pytest.skip("Notebook tester not available")

        imports = notebook_tester.extract_imports()
        import_text = " ".join(imports)

        # Check for key imports
        assert "fluxnet_shuttle_lib" in import_text, "Should import fluxnet_shuttle_lib"
        assert "pandas" in import_text, "Should import pandas"
        assert "matplotlib" in import_text, "Should import matplotlib"

    def test_import_validation(self, notebook_tester):
        """Test that imports can be executed successfully."""
        if notebook_tester is None:
            pytest.skip("Notebook tester not available")

        import_results = notebook_tester.validate_imports()

        # Check critical imports
        critical_imports = [
            "import os",
            "import logging",
            "import zipfile",
            "import pandas as pd",
        ]

        for critical_import in critical_imports:
            matching_imports = [imp for imp in import_results.keys() if critical_import in imp]
            if matching_imports:
                # At least one matching import should succeed
                assert any(
                    import_results[imp] for imp in matching_imports
                ), f"Critical import failed: {critical_import}"

    def test_package_availability(self, notebook_tester):
        """Test that required packages are available."""
        if notebook_tester is None:
            pytest.skip("Notebook tester not available")

        packages = notebook_tester.check_required_packages()

        # Check built-in packages
        assert packages["zipfile"], "zipfile should be available (built-in)"
        assert packages["logging"], "logging should be available (built-in)"

        # Check external packages (may not be installed in test environment)
        if packages["pandas"]:
            print("✓ pandas is available")
        else:
            print("⚠ pandas not available - install with: pip install pandas")

        if packages["matplotlib"]:
            print("✓ matplotlib is available")
        else:
            print("⚠ matplotlib not available - install with: pip install matplotlib")

    @pytest.mark.slow
    @pytest.mark.integration
    def test_notebook_execution(self, notebook_tester):
        """Test that the notebook can be executed (slow integration test)."""
        if notebook_tester is None:
            pytest.skip("Notebook tester not available")

        # Check if jupyter is available
        try:
            subprocess.run([sys.executable, "-m", "jupyter", "--version"], capture_output=True, check=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            pytest.skip("Jupyter not available for notebook execution test")

        # This test is marked as slow because it would execute the entire notebook
        # including API calls which can take several minutes
        # Execute notebook with extended timeout and ensure we wait for completion
        success, error_message = notebook_tester.execute_notebook(timeout=1200)  # 20 minute timeout

        # Additional verification that the process completed
        if success:
            print("✓ Notebook executed successfully")
        else:
            print("✗ Notebook execution failed or timed out")
            print(f"Error details:\n{error_message}")

        if not success:
            pytest.fail(f"Notebook execution failed: {error_message}")
        else:
            assert success, "Notebook should execute successfully"


if __name__ == "__main__":
    # Run tests for the example notebook
    pytest.main([__file__, "-v", "--tb=short"])
