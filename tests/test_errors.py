from scam_sniffer.errors import ScamError
from scam_sniffer.domain.errors import ManagerError
from scam_sniffer.data.api.client.errors import ApiError
from scam_sniffer.data.api.stock.errors import StockError
from scam_sniffer.data.database.errors import DatabaseError
from scam_sniffer.domain.errors import DomainError

def test_custom_errors_inherit_scam_error() -> None:
    assert issubclass(ApiError, ScamError)
    assert issubclass(DomainError, ScamError)
    assert issubclass(StockError, ScamError)
    assert issubclass(ManagerError, ScamError)
    assert issubclass(DatabaseError, ScamError)
