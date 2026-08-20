from scam_sniffer.domain.errors import ScamError
from scam_sniffer.data.api.client.errors import ApiError
from scam_sniffer.data.api.stock.errors import StockError
from scam_sniffer.data.database.errors import DatabaseError
from scam_sniffer.domain.repository.errors import RepoError

def test_custom_errors_inherit_scam_error() -> None:
    assert issubclass(ApiError, ScamError)
    assert issubclass(RepoError, ScamError)
    assert issubclass(StockError, ScamError)
    assert issubclass(DatabaseError, ScamError)
