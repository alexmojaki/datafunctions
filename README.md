# datafunctions

[![Build Status](https://travis-ci.org/alexmojaki/datafunctions.svg?branch=master)](https://travis-ci.org/alexmojaki/datafunctions) [![Coverage Status](https://coveralls.io/repos/github/alexmojaki/datafunctions/badge.svg?branch=master)](https://coveralls.io/github/alexmojaki/datafunctions?branch=master) [![Supports Python versions 3.7+](https://img.shields.io/pypi/pyversions/datafunctions.svg)](https://pypi.python.org/pypi/datafunctions)

Automatic (de)serialization of arguments and return values for Python functions.

    pip install datafunctions

`@datafunction` is a decorator which automatically deserializes incoming arguments of the decorated function and
serializes the return value. For example:

```python
from datetime import datetime
from datafunctions import datafunction

@datafunction
def next_year(dt: datetime) -> datetime:
    return dt.replace(year=dt.year + 1)

assert next_year("2019-01-02T00:00:00") == "2020-01-02T00:00:00"
```

`@datafunction` automatically converts the string argument to a datetime object, and then
converts the returned datetime back to a string.

More generally, the arguments and return value as seen from the outside the function
are basic JSON serializable objects - strings, dicts, etc.
They are converted to and from the correct types (as indicated by type annotations)
by [marshmallow](https://marshmallow.readthedocs.io/). Common Python types as well as dataclasses (which may be nested)
are supported. For example:

```python
from dataclasses import dataclass
from datafunctions import datafunction

@dataclass
class Point:
    x: int
    y: int

@datafunction
def translate(p: Point, dx: int, dy: int) -> Point:
    return Point(p.x + dx, p.y + dy)

assert translate({"x": 1, "y": 2}, 3, 4) == {"x": 4, "y": 6}
```

To decorate a method, pass `is_method=True`, e.g:

```python
class MyClass:
    @datafunction(is_method=True)
    def method(self, x: int) -> int:
        ...
```

All parameters and the return value must have a type annotation,
except for the first argument when `is_method=True`.
Variadic parameters (e.g. `*args` or `**kwargs`) and positional-only parameters (before `/`)
are not allowed.

If there is an exception deserializing or binding the arguments an `ArgumentError`
will be raised with the underlying exception attached to `__cause__`.
Similarly a `ReturnError` may be raised when trying to serialize the return value.

For more manual control, use the methods:

- `load_arguments`
- `dump_arguments`
- `load_result`
- `dump_result`

Under the hood, the type annotations are gathered into a [dataclass](https://docs.python.org/3/library/dataclasses.html) which is then
converted into a [marshmallow](https://marshmallow.readthedocs.io/en/stable/) schema
using [marshmallow_dataclass](https://github.com/lovasoa/marshmallow_dataclass).
marshmallow handles the (de)serialization.

Instances of this class have attributes `params_schemas` and `return_schemas`,
each of which have the following attributes:

- `dataclass`
- `schema_class`: the marshmallow schema class
- `schema_instance`: a no-args instance of schema_class
