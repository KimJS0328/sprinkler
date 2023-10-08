
class A:
    def a(self, b: int) -> None:
        print()

from inspect import Signature

sig = Signature.from_callable(A().a)

print(bool(sig.parameters['b'].annotation))
print(sig.parameters['b'].default)
print(sig.return_annotation)

from typing import get_type_hints
print(get_type_hints(A().a))
