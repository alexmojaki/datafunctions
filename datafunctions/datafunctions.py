import functools
import inspect
from collections import namedtuple
from dataclasses import dataclass
from functools import lru_cache, partial
from typing import get_type_hints

import marshmallow_dataclass
from marshmallow import ValidationError


class ArgumentError(Exception):
    pass


class ReturnError(Exception):
    pass


class _datafunction_meta(type):
    def __call__(self, func=None, *, is_method=False):
        if func is not None:
            return super().__call__(func, is_method=is_method)

        return partial(datafunction, is_method=is_method)


@lru_cache()
class datafunction(metaclass=_datafunction_meta):
    def __init__(self, func=None, *, is_method: bool = False):
        self.func = func
        self.is_method = is_method
        functools.update_wrapper(self, func)
        self.hints = get_type_hints(self.func)
        self.signature = inspect.signature(self.func)

        self.hinted_names = list(self.signature.parameters)
        if self.is_method:
            del self.hinted_names[0]

        for name in [*self.hinted_names, "return"]:
            if name not in self.hints:
                raise TypeError(f"Missing annotation for {name}")

        for name in self.hinted_names:
            param = self.signature.parameters[name]
            if param.kind not in (
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    inspect.Parameter.KEYWORD_ONLY,
            ):
                raise TypeError(f"Parameter {name} is of invalid kind: {param.kind.name}")

        def make_schema(label, annotations):
            cls = type(f"{self.func.__name__}_{label}_schema", (), {"__annotations__": annotations})
            datacls = dataclass(cls)
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
        @functools.wraps(self)
        def method(instance_self, *args, **kwargs):
            return self(instance_self, *args, **kwargs)

        return method.__get__(instance, owner)

    def dump_arguments(self, *args, **kwargs):
        try:
            hinted_arguments, all_arguments = self.arguments_dicts(args, kwargs)
            return self.params_schemas.schema_instance.dump(hinted_arguments)
        except Exception as e:
            raise ArgumentError from e

    def load_arguments(self, *args, **kwargs):
        try:
            hinted_arguments, all_arguments = self.arguments_dicts(args, kwargs)
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

    def arguments_dicts(self, args, kwargs):
        bound_arguments = self.signature.bind(*args, **kwargs)
        bound_arguments.apply_defaults()
        all_arguments = bound_arguments.arguments
        hinted_arguments = {
            k: all_arguments[k]
            for k in self.hinted_names
        }
        return hinted_arguments, all_arguments

    def dump_result(self, result):
        if self.return_schemas is None:
            return None

        try:
            result_data = self.return_schemas.schema_instance.dump({"_return": result})
        except Exception as e:
            raise ReturnError from e

        return result_data["_return"]

    def load_result(self, result):
        if self.return_schemas is None:
            return None

        try:
            datacls_instance = self.return_schemas.schema_instance.load({"_return": result})
        except ValidationError as e:
            raise ReturnError from e

        return datacls_instance._return


Schemas = namedtuple(
    "Schemas",
    "dataclass schema_class schema_instance"
)
