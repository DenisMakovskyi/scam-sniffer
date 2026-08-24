from scam_sniffer.errors import AppError

from scam_sniffer.core.tasks.errors import TaskQueueError
from scam_sniffer.core.log.errors import LogError

from scam_sniffer.data.api.client.errors import ApiError
from scam_sniffer.data.api.stock.errors import StockError
from scam_sniffer.data.database.errors import DatabaseError

from scam_sniffer.domain.errors import DomainError, ManagerError

def test_custom_errors_inherit_app_error() -> None:
    assert issubclass(ApiError, AppError)
    assert issubclass(LogError, AppError)
    assert issubclass(DomainError, AppError)
    assert issubclass(StockError, AppError)
    assert issubclass(ManagerError, AppError)
    assert issubclass(TaskQueueError, AppError)
    assert issubclass(DatabaseError, AppError)
