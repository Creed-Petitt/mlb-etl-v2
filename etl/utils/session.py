from contextlib import contextmanager
from models import get_session as get_base_session

def get_session():
    return get_base_session()

@contextmanager
def get_managed_session():
    session = get_session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

@contextmanager
def get_transaction_session():
    session = get_session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()