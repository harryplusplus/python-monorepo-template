from enum import Enum


class Propagation(str, Enum):
    # Support a current transaction, create a new one if none exists.
    REQUIRED = "required"

    # Support a current transaction, throw an exception if none exists.
    MANDATORY = "mandatory"

    # Create a new transaction, and suspend the current transaction if one exists.
    REQUIRES_NEW = "requires_new"

    # Execute within a nested transaction if a current transaction exists, behave like REQUIRED otherwise.
    NESTED = "nested"


class SQLAlchemyTransactionalError(Exception):
    default_message = "sqlalchemy_transactional error."

    def __init__(self, message: str | None = None) -> None:
        super().__init__(message or self.default_message)


class SessionFactoryAlreadyBoundError(SQLAlchemyTransactionalError):
    default_message = "Session factory is already bound to the current context."


class SessionFactoryNotBoundError(SQLAlchemyTransactionalError):
    default_message = "Session factory is not bound to the current context."


class SessionAlreadyBoundError(SQLAlchemyTransactionalError):
    default_message = "Session is already bound to the current context."


class SessionNotBoundError(SQLAlchemyTransactionalError):
    default_message = "Session is not bound to the current context."


class TransactionRequiredError(SQLAlchemyTransactionalError):
    default_message = "An active transaction is required in the current context."


class UnsupportedPropagationModeError(SQLAlchemyTransactionalError):
    def __init__(self, propagation: Propagation) -> None:
        self.propagation: Propagation = propagation
        super().__init__(f"Unsupported propagation mode: {propagation.value!r}.")
