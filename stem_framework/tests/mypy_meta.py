import dataclasses

from stem.meta import Meta, Specification, get_meta_attr, MetaVerification


@dataclasses.dataclass
class Example:
    a: int = 0
    b: float = 0.0
    c: list = dataclasses.field(default_factory=list)


if __name__ == '__main__':
    get_meta_attr(Example(), "a")
    get_meta_attr({}, "a")
    specification = (("a", int), ("b", (int, float)))
    MetaVerification.verify(Example(), Example)
    MetaVerification.verify({}, Example)
    MetaVerification.verify(Example(), specification)
    MetaVerification.verify({}, specification)