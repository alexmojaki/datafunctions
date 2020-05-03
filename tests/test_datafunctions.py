import dataclasses
from datetime import datetime
from typing import Optional

import marshmallow
import pytest
from marshmallow import ValidationError

from datafunctions import datafunction, ArgumentError, ReturnError
from .utils import raises_with_cause


@dataclasses.dataclass
class Point:
    x: int
    y: int


def test_simple():
    @datafunction
    def next_year(dt: datetime) -> datetime:
        return dt.replace(year=dt.year + 1)

    # noinspection PyTypeChecker
    assert next_year("2019-01-02T00:00:00") == "2020-01-02T00:00:00"


def test_unbound_method():
    class A:
        @datafunction(is_method=True)
        def foo(self, x: int) -> int:
            return x * 2

    # noinspection PyTypeChecker
    assert A().foo("3") == A.foo(A(), "3") == 6

    assert A.foo.hinted_names == ["x"]


def test_bound_method():
    class A:
        def foo(self, x: int) -> int:
            return x * 2

    method = A().foo
    decorated = datafunction(method)
    assert decorated("3") == 6


def test_attributes():
    @datafunction()
    def foo(x: int, y: str) -> bool:
        return x == y

    assert foo.func == foo.__wrapped__
    assert not foo.is_method
    assert foo.hints == {"x": int, "y": str, "return": bool}
    assert str(foo.signature) == "(x: int, y: str) -> bool"
    assert foo.hinted_names == ["x", "y"]

    assert dataclasses.fields(foo.params_schemas.dataclass)[0].name == "x"
    assert isinstance(foo.params_schemas.schema_class._declared_fields["x"], marshmallow.fields.Integer)
    assert isinstance(foo.params_schemas.schema_instance, foo.params_schemas.schema_class)

    assert dataclasses.fields(foo.return_schemas.dataclass)[0].name == "_return"
    assert isinstance(foo.return_schemas.schema_class._declared_fields["_return"], marshmallow.fields.Bool)
    assert isinstance(foo.return_schemas.schema_instance, foo.return_schemas.schema_class)


def test_dataclass_args():
    @datafunction()
    def translate(p: Point, dx: int, dy: int) -> Point:
        return Point(p.x + dx, p.y + dy)

    assert (
            translate.dump_arguments(Point(1, 2), 3, 4) ==
            translate.dump_arguments(p=Point(1, 2), dx=3, dy=4) ==
            {"p": {"x": 1, "y": 2}, "dx": 3, "dy": 4}
    )

    assert (
            translate.load_arguments({"x": 1, "y": 2}, 3, 4) ==
            translate.load_arguments(p={"x": 1, "y": 2}, dx=3, dy=4) ==
            {"p": Point(1, 2), "dx": 3, "dy": 4}
    )

    assert translate.dump_result(Point(1, 2)) == {"x": 1, "y": 2}
    assert translate.load_result({"x": 1, "y": 2}) == Point(1, 2)

    # noinspection PyTypeChecker
    assert (
            translate({"x": 1, "y": 2}, 3, 4) ==
            translate({"x": 1, "y": 2}, dx=3, dy=4) ==
            translate(p={"x": 1, "y": 2}, dx=3, dy=4) ==
            {"x": 4, "y": 6}
    )


def test_default_args():
    @datafunction
    def foo(x: int, y: int = 0, z: int = 1) -> int:
        return x + y + z

    assert foo(3) == 4
    assert foo(3, 2) == 6
    assert foo(3, 2, 5) == 10
    # noinspection PyTypeChecker
    assert foo("3", "2", "5") == 10
    assert foo(x=3, y=2, z=5) == 10

    with raises_with_cause(ArgumentError, TypeError, "missing a required argument: 'x'"):
        foo(y=4)


# noinspection PyTypeChecker
def test_argument_validation():
    @datafunction
    def foo(x: int) -> int:
        return x

    for func in [
        lambda: foo("abc"),
        lambda: foo.load_arguments("abc"),
    ]:
        with raises_with_cause(ArgumentError, ValidationError, "{'x': ['Not a valid integer.']}"):
            func()

    with raises_with_cause(ArgumentError, ValueError, "invalid literal for int() with base 10: 'abc'"):
        foo.dump_arguments("abc")


def test_missing_annotation():
    with pytest.raises(TypeError, match="Missing annotation for x"):
        @datafunction
        def foo(x, y: int) -> int:
            return x + y

    with pytest.raises(TypeError, match="Missing annotation for return"):
        @datafunction
        def foo(x: int, y: int):
            return x + y


def test_invalid_parameter_kind():
    with pytest.raises(TypeError, match="Parameter args is of invalid kind: VAR_POSITIONAL"):
        @datafunction
        def foo(*args: tuple) -> tuple:
            return args

    with pytest.raises(TypeError, match="Parameter args is of invalid kind: VAR_KEYWORD"):
        @datafunction
        def foo(**args: tuple) -> dict:
            return args


def test_return_validation():
    @datafunction
    def foo(x: int) -> int:
        # noinspection PyTypeChecker
        return f"abc{x}"

    for func in [
        lambda: foo(3),
        lambda: foo.dump_result("abc3"),
    ]:
        with raises_with_cause(ReturnError, ValueError, "invalid literal for int() with base 10: 'abc3'"):
            func()

    with raises_with_cause(ReturnError, ValidationError, "{'_return': ['Not a valid integer.']}"):
        foo.load_result("abc3")


def test_returns_none():
    @datafunction()
    def foo(_x: int) -> None:
        pass

    assert foo(3) is None
    assert foo.return_schemas is None
    assert foo.dump_result("sfoo") is None
    assert foo.load_result("sfoo") is None


def test_optional():
    @datafunction()
    def foo(x: Optional[int]) -> Optional[int]:
        return x

    for value in [3, None]:
        for func in [
            foo,
            foo.dump_result,
            foo.load_result,
            lambda v: foo.dump_arguments(v)["x"],
            lambda v: foo.load_arguments(v)["x"],
        ]:
            assert func(value) == value


def test_caching():
    def foo(x: int) -> int:
        return x

    assert datafunction(foo) is datafunction(foo)
