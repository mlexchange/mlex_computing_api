from fastapi.testclient import TestClient
import pytest
import mongomock

from main import app, set_compute_service, svc_context
from job_service import ComputeService


@pytest.fixture(scope="module")
def mongodb():
    return mongomock.MongoClient().db


@pytest.fixture(scope="module")
def comp_svc(mongodb):
    comp_svc = ComputeService(mongodb)
    return comp_svc


@pytest.fixture(scope="module")
def rest_client(comp_svc):
    set_compute_service(comp_svc)
    return TestClient(app)
