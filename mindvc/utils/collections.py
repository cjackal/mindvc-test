from typing import Iterable, List, Union


def ensure_list(item: Union[Iterable[str], str, None]) -> List[str]:
    if item is None:
        return []
    if isinstance(item, str):
        return [item]
    return list(item)
