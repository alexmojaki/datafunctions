import functools
import inspect
from dataclasses import make_dataclass
from functools import lru_cache, partial
from typing import get_type_hints, NamedTuple, Type, Callable, Dict, Any, Tuple

import marshmallow
import marshmallow_dataclass
from marshmallow import ValidationError


class ArgumentError(Exception):
    """
    Raised by datafunction.load/dump_arguments when the arguments are invalid,
    e.g. if they cannot be bound to the function parameters
    or they fail marshmallow validation.

    There will always be an underlying exception in the __cause__ attribute.
    """


class ReturnError(Exception):
    """
    Raised by datafunction.load/dump_result when the return value is invalid,
    e.g. if fail marshmallows validation.

    There will always be an underlying exception in the __cause__ attribute.
    """


class Schemas(NamedTuple):
    dataclass: type
    schema_class: Type[marshmallow.Schema]
    schema_instance: marshmallow.Schema


class _datafunction_meta(type):
    """
    Metaclass which allows datafunction to be used as a decorator
    with or without arguments.
    """

    def __call__(self, func=None, *, is_method=False):
        if func is not None:
            return super().__call__(func, is_method=is_method)

        return partial(datafunction, is_method=is_method)


@lru_cache()
class datafunction(metaclass=_datafunction_meta):
    """
    @datafunction is a decorator 
    which automatically deserializes incoming arguments of the decorated function and
    serializes the return value. For example::

        from datetime import datetime

        @datafunction
        def next_year(dt: datetime) -> datetime:
            return dt.replace(year=dt.year + 1)

        assert next_year("2019-01-02T00:00:00") == "2020-01-02T00:00:00"

    @datafunction automatically converts the string argument to a datetime object, and then
    converts the returned datetime back to a string.

    More generally, the arguments and return value as seen from the outside the function
    are basic JSON serializable objects - strings, dicts, etc.
    They are converted to and from the correct types (as indicated by type annotations)
    by marshmallow. Common Python types as well as dataclasses (which may be nested)
    are supported. For example::

        @dataclass
        class Point:
            x: int
            y: int

        @datafunction
        def translate(p: Point, dx: int, dy: int) -> Point:
            return Point(p.x + dx, p.y + dy)

        assert translate({"x": 1, "y": 2}, 3, 4) == {"x": 4, "y": 6}

    To decorate a method, pass is_method=True, e.g::

        class MyClass:
            @datafunction(is_method=True)
            def method(self, x: int) -> int:
                ...

    All parameters and the return value must have a type annotation,
    except for the first argument when is_method=True.
    Variadic parameters (*args, **kwargs) and positional-only parameters (before /)
    are not allowed.

    If there is an exception deserializing or binding the arguments an ArgumentError
    will be raised with the underlying exception attached to __cause__.
    Similarly a ReturnError may be raised when trying to serialize the return value.

    For more manual control, use the methods:
        load_arguments
        dump_arguments
        load_result
        dump_result

    Under the hood, the type annotations are gathered into a dataclass which is then
    converted into a marshmallow schema
    using https://github.com/lovasoa/marshmallow_dataclass
    which handles the (de)serialization.

    Instances of this class have attributes params_schemas and return_schemas,
    each of which have the following attributes:
        dataclass
        schema_class: the marshmallow schema class
        schema_instance: a no-args instance of schema_class
    """

    def __init__(self, func: Callable = None, *, is_method: bool = False):
        self.func = func
        self.is_method = is_method
        functools.update_wrapper(self, func)
        self.hints = get_type_hints(self.func)
        self.signature = inspect.signature(self.func)

        self.hinted_names = list(self.signature.parameters)
        if self.is_method:
            # The first argument of a method (e.g. self)
            # does not need to be hinted as it will not be deserialized
            del self.hinted_names[0]

        for name in [*self.hinted_names, "return"]:
            if name not in self.hints:
                raise TypeError(f"Missing annotation for {name} in function {func.__name__}")

        for name in self.hinted_names:
            param = self.signature.parameters[name]
            if param.kind not in (
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    inspect.Parameter.KEYWORD_ONLY,
            ):
                raise TypeError(f"Parameter {name} in function {func.__name__} is of invalid kind: {param.kind.name}")

        def make_schema(label, fields):
            datacls = make_dataclass(f"{self.func.__name__}_{label}_schema", fields.items())
            schema = marshmallow_dataclass.class_schema(datacls)
            schema_instance = schema()
            return Schemas(datacls, schema, schema_instance)

        self.params_schemas = make_schema("params", {k: self.hints[k] for k in self.hinted_names})
        self.return_schemas = make_schema("return", {"_return": self.hints["return"]}) \
            if self.hints["return"] != type(None) else None

    def __call__(self, *args, **kwargs):
        data = self.load_arguments(*args, **kwargs)
        result = self.func(**data)
        return self.dump_result(result)

    def __get__(self, instance, owner):
        # Ensure method binding works correctly
        @functools.wraps(self)
        def method(instance_self, *args, **kwargs):
            return self(instance_self, *args, **kwargs)

        return method.__get__(instance, owner)

    def dump_arguments(self, *args, **kwargs) -> Dict[str, Any]:
        """
        Returns a dictionary containing JSON serializable values converted
        from the arguments by the .dump() method of the marshmallow schema
        derived from the parameter annotations.

        For example:

            @dataclass
            class Point:
                x: int
                y: int

            @datafunction
            def translate(p: Point, dx: int, dy: int) -> Point:
                return Point(p.x + dx, p.y + dy)

            assert (
                translate.dump_arguments(Point(1, 2), 3, 4) ==
                translate.dump_arguments(p=Point(1, 2), dx=3, dy=4) ==
                {"p": {"x": 1, "y": 2}, "dx": 3, "dy": 4}
            )
        """
        try:
            hinted_arguments, all_arguments = self._arguments_dicts(args, kwargs)
            # Only the hinted_arguments (i.e. not 'self') can be serialized
            return self.params_schemas.schema_instance.dump(hinted_arguments)
        except Exception as e:
            raise ArgumentError from e

    def load_arguments(self, *args, **kwargs) -> Dict[str, Any]:
        """
        Returns a dictionary of named deserialized arguments converted
        from the given serialized arguments.
        The conversion is done by the .load() method of the marshmallow schema
        derived from the parameter annotations.

        For example::

            @dataclass
            class Point:
                x: int
                y: int

            @datafunction
            def translate(p: Point, dx: int, dy: int) -> Point:
                return Point(p.x + dx, p.y + dy)

            assert (
                translate.load_arguments({"x": 1, "y": 2}, 3, 4) ==
                translate.load_arguments(p={"x": 1, "y": 2}, dx=3, dy=4) ==
                {"p": Point(1, 2), "dx": 3, "dy": 4}
            )
        """
        try:
            hinted_arguments, all_arguments = self._arguments_dicts(args, kwargs)
            datacls_instance = self.params_schemas.schema_instance.load(hinted_arguments)

            return {
                **all_arguments,
                **{
                    field: getattr(datacls_instance, field)
                    for field in self.hinted_names
                }
            }
        except (TypeError, ValidationError) as e:
            raise ArgumentError from e

    def _arguments_dicts(self, args, kwargs) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        bound_arguments = self.signature.bind(*args, **kwargs)
        bound_arguments.apply_defaults()
        all_arguments = bound_arguments.arguments
        hinted_arguments = {
            k: all_arguments[k]
            for k in self.hinted_names
        }
        return hinted_arguments, all_arguments

    def dump_result(self, result):
        """
        Returns a JSON serializable version of the given value
        returned from the decorated function.
        The conversion is done by the .dump() method of the marshmallow schema
        derived from the return annotation.

        For example::

            @dataclass
            class Point:
                x: int
                y: int

            @datafunction
            def translate(p: Point, dx: int, dy: int) -> Point:
                return Point(p.x + dx, p.y + dy)

            assert translate.dump_result(Point(1, 2)) == {"x": 1, "y": 2}
        """
        if self.return_schemas is None:
            return None

        try:
            result_data = self.return_schemas.schema_instance.dump({"_return": result})
        except Exception as e:
            raise ReturnError from e

        return result_data["_return"]

    def load_result(self, result):
        """
        Deserializes the given serialized value representing a return from the function.
        The conversion is done by the .load() method of the marshmallow schema
        derived from the return annotation.

        For example::

            @dataclass
            class Point:
                x: int
                y: int

            @datafunction
            def translate(p: Point, dx: int, dy: int) -> Point:
                return Point(p.x + dx, p.y + dy)

            assert translate.load_result({"x": 1, "y": 2}) == Point(1, 2)
        """

        if self.return_schemas is None:
            return None

        try:
            datacls_instance = self.return_schemas.schema_instance.load({"_return": result})
        except ValidationError as e:
            raise ReturnError from e

        return datacls_instance._return
