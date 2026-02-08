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
