import errno
import os
import ntpath
import posixpath
from urllib.parse import urlsplit, urlunsplit
from types import ModuleType
from typing import (
    TYPE_CHECKING,
    Any,
    BinaryIO,
    Callable,
    Dict,
    Iterable,
    Iterator,
    Optional,
    Sequence,
    Tuple,
)

from fsspec.spec import AbstractFileSystem

if TYPE_CHECKING:
    from io import BytesIO

    from .git import Git
    from .git.objects import GitTrie


class Path:
    def __init__(
        self,
        sep: str,
        getcwd: Optional[Callable[[], str]] = None,
        realpath: Optional[Callable[[str], str]] = None,
    ):
        def _getcwd() -> str:
            return ""

        self.getcwd: Callable[[], str] = getcwd or _getcwd
        self.realpath = realpath or self.abspath

        if sep == posixpath.sep:
            self.flavour: ModuleType = posixpath
        elif sep == ntpath.sep:
            self.flavour = ntpath
        else:
            raise ValueError(f"unsupported separator '{sep}'")

    def chdir(self, path: str):
        def _getcwd() -> str:
            return path

        self.getcwd = _getcwd

    def join(self, *parts: str) -> str:
        return self.flavour.join(*parts)

    def split(self, path: str) -> Tuple[str, str]:
        return self.flavour.split(path)

    def splitext(self, path: str) -> Tuple[str, str]:
        return self.flavour.splitext(path)

    def normpath(self, path: str) -> str:
        if self.flavour == ntpath:
            return self.flavour.normpath(path)

        parts = list(urlsplit(path))
        parts[2] = self.flavour.normpath(parts[2])
        return urlunsplit(parts)

    def isabs(self, path: str) -> bool:
        return self.flavour.isabs(path)

    def abspath(self, path: str) -> str:
        if not self.isabs(path):
            path = self.join(self.getcwd(), path)
        return self.normpath(path)

    def commonprefix(self, paths: Sequence[str]) -> str:
        return self.flavour.commonprefix(paths)

    def commonpath(self, paths: Iterable[str]) -> str:
        return self.flavour.commonpath(paths)

    def parts(self, path: str) -> Tuple[str, ...]:
        drive, path = self.flavour.splitdrive(path.rstrip(self.flavour.sep))

        ret = []
        while True:
            path, part = self.flavour.split(path)

            if part:
                ret.append(part)
                continue

            if path:
                ret.append(path)

            break

        ret.reverse()

        if drive:
            ret = [drive] + ret

        return tuple(ret)

    def parent(self, path: str) -> str:
        return self.flavour.dirname(path)

    def dirname(self, path: str) -> str:
        return self.parent(path)

    def parents(self, path: str) -> Iterator[str]:
        while True:
            parent = self.flavour.dirname(path)
            if parent == path:
                break
            yield parent
            path = parent

    def name(self, path: str) -> str:
        return self.flavour.basename(path)

    def suffix(self, path: str) -> str:
        name = self.name(path)
        _, dot, suffix = name.partition(".")
        return dot + suffix

    def with_name(self, path: str, name: str) -> str:
        return self.join(self.parent(path), name)

    def with_suffix(self, path: str, suffix: str) -> str:
        return self.splitext(path)[0] + suffix

    def isin(self, left: str, right: str) -> bool:
        if left == right:
            return False
        try:
            common = self.commonpath([left, right])
        except ValueError:
            # Paths don't have the same drive
            return False
        return common == right

    def isin_or_eq(self, left: str, right: str) -> bool:
        return left == right or self.isin(left, right)

    def overlaps(self, left: str, right: str) -> bool:
        # pylint: disable=arguments-out-of-order
        return self.isin_or_eq(left, right) or self.isin(right, left)

    def relpath(self, path: str, start: Optional[str] = None) -> str:
        if start is None:
            start = "."
        return self.flavour.relpath(
            self.abspath(path), start=self.abspath(start)
        )

    def relparts(
        self, path: str, start: Optional[str] = None
    ) -> Tuple[str, ...]:
        return self.parts(self.relpath(path, start=start))

    def as_posix(self, path: str) -> str:
        return path.replace(self.flavour.sep, posixpath.sep)


def bytesio_len(obj: "BytesIO") -> Optional[int]:
    try:
        offset = obj.tell()
        length = obj.seek(0, os.SEEK_END)
        obj.seek(offset)
    except (AttributeError, OSError):
        return None
    return length


class GitFileSystem(AbstractFileSystem):
    # pylint: disable=abstract-method
    cachable = False
    root_marker = "/"

    def __init__(
        self,
        path: str = None,
        rev: str = None,
        scm: "Git" = None,
        trie: "GitTrie" = None,
        rev_resolver: Callable[["Git", str], str] = None,
        **kwargs,
    ):
        from .git import Git
        from .git.objects import GitTrie

        super().__init__(**kwargs)
        if not trie:
            scm = scm or Git(path)
            resolver = rev_resolver or Git.resolve_rev
            resolved = resolver(scm, rev or "HEAD")
            tree_obj = scm.get_tree_obj(rev=resolved)
            trie = GitTrie(tree_obj, resolved)

        self.trie = trie
        self.rev = self.trie.rev

        def _getcwd():
            return self.root_marker

        self.path = Path(self.sep, getcwd=_getcwd)

    def _get_key(self, path: str) -> Tuple[str, ...]:
        path = self.path.abspath(path)
        if path == self.root_marker:
            return ()
        relparts = path.split(self.sep)
        if relparts and relparts[0] in (".", ""):
            relparts = relparts[1:]
        return tuple(relparts)

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: int = None,
        autocommit: bool = True,
        cache_options: Dict = None,
        **kwargs: Any,
    ) -> BinaryIO:
        if mode != "rb":
            raise NotImplementedError

        key = self._get_key(path)
        try:
            obj = self.trie.open(key, mode=mode)
            obj.size = bytesio_len(obj)
            return obj
        except KeyError as exc:
            msg = os.strerror(errno.ENOENT) + f" in branch '{self.rev}'"
            raise FileNotFoundError(errno.ENOENT, msg, path) from exc
        except IsADirectoryError as exc:
            raise IsADirectoryError(
                errno.EISDIR, os.strerror(errno.EISDIR), path
            ) from exc

    def info(self, path: str, **kwargs: Any) -> Dict[str, Any]:
        key = self._get_key(path)
        try:
            # NOTE: to avoid wasting time computing object size, trie.info
            # will return a LazyDict instance, that will compute compute size
            # only when it is accessed.
            ret = self.trie.info(key)
            ret["name"] = path
            return ret
        except KeyError:
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), path
            )

    def exists(self, path: str, **kwargs: Any) -> bool:
        key = self._get_key(path)
        return self.trie.exists(key)

    def checksum(self, path: str) -> str:
        return self.info(path)["sha"]

    def ls(self, path, detail=True, **kwargs):
        info = self.info(path)
        if info["type"] != "directory":
            return [info] if detail else [path]

        key = self._get_key(path)
        try:
            names = self.trie.ls(key)
        except KeyError as exc:
            raise FileNotFoundError from exc

        paths = [
            posixpath.join(path, name) if path else name for name in names
        ]

        if not detail:
            return paths

        return [self.info(_path) for _path in paths]
