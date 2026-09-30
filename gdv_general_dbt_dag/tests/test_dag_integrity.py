import os
import sys
import pytest
from airflow.models import DagBag

# Add the project root to sys.path to allow importing modules
sys.path.append(os.path.join(os.path.dirname(__file__), "../"))

def test_dag_integrity():
    """
    Verify that all DAGs in the repository can be loaded without errors.
    Checks for import errors and cycles.
    """
    # Point to the dags directory (parent of tests/)
    dags_folder = os.path.join(os.path.dirname(__file__), "../")
    
    dag_bag = DagBag(dag_folder=dags_folder, include_examples=False)
    
    # Check for import errors
    assert not dag_bag.import_errors, f"DAG import errors: {dag_bag.import_errors}"
    
    # Check that we actually found some DAGs (sanity check)
    assert len(dag_bag.dags) > 0, "No DAGs found in the repository"
