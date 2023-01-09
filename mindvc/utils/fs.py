import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..types import StrPath


def path_isin(child: "StrPath", parent: "StrPath") -> bool:
    """Check if given `child` path is inside `parent`."""

    def normalize_path(path) -> str:
        return os.path.normcase(os.path.normpath(path))

    parent = os.path.join(normalize_path(parent), "")
    child = normalize_path(child)
    return child != parent and child.startswith(parent)
