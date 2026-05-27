import sys
from typing import Type, Any, Optional, TypeVar

E = TypeVar('E', bound=BaseException)

class AvroException(Exception):
    pass
class NoneErrorException(Exception):
    pass
class SchemaFieldMissingException(Exception):
    pass
class DefaultValueException(Exception):
    pass

class InvalidSchemaException(Exception):
    pass

def handle_exception(exceptionType: Type[E], exceptionValue: E, exc_traceback: Optional[Any]) -> None:
    if issubclass(exceptionType, AvroException):
        print(f"AvroException: {exceptionValue}")
    elif issubclass(exceptionType, NoneErrorException):
        print(f"NoneErrorException: {exceptionValue}")
    elif issubclass(exceptionType, SchemaFieldMissingException):
        print(f"SchemaFieldMissingException: {exceptionValue}")
    elif issubclass(exceptionType, DefaultValueException):
        print(f"DefaultValueException: {exceptionValue}")
    elif issubclass(exceptionType, InvalidSchemaException):
        print(f"InvalidSchemaException: {exceptionValue}")
    else:
        print(f"Invalid JSON syntax: JSON structure is not proper either is missing or braces are not closed properly: {exceptionValue}")

sys.excepthook = handle_exception
