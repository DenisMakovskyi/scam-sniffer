from scam_sniffer.domain.errors import DomainError
from scam_sniffer.errors import AppError
from scam_sniffer.domain.errors import ManagerError
from scam_sniffer.data.api.client.errors import ApiError
from scam_sniffer.data.api.stock.errors import StockError
from scam_sniffer.core.tasks.errors import TaskQueueError
from scam_sniffer.data.database.errors import DatabaseError

def test_custom_errors_inherit_scam_error() -> None:
    assert issubclass(ApiError, AppError)
    assert issubclass(DomainError, AppError)
    assert issubclass(StockError, AppError)
    assert issubclass(ManagerError, AppError)
    assert issubclass(TaskQueueError, AppError)
    assert issubclass(DatabaseError, AppError)
