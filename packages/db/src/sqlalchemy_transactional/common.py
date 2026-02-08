from enum import Enum


class Propagation(str, Enum):
    # Support a current transaction, throw an exception if none exists.
    MANDATORY = "mandatory"

    # Execute within a nested transaction if a current transaction exists, behave like REQUIRED otherwise.
    NESTED = "nested"

    # Execute non-transactionally, throw an exception if a transaction exists.
    NEVER = "never"

    # Execute non-transactionally, suspend the current transaction if one exists.
    NOT_SUPPORTED = "not_supported"

    # Support a current transaction, create a new one if none exists.
    REQUIRED = "required"

    # Create a new transaction, and suspend the current transaction if one exists.
    REQUIRES_NEW = "requires_new"

    # Support a current transaction, execute non-transactionally if none exists.
    SUPPORTS = "supports"
