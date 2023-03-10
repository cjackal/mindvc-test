from collections.abc import Mapping
from typing import TYPE_CHECKING, Iterator, Type
from urllib.parse import urlparse

from . import generic  # noqa: F401
from .git import GitFileSystem
from .local import LocalFileSystem, localfs  # noqa: F401
from .memory import MemoryFileSystem  # noqa: F401
from .scheme import Schemes

if TYPE_CHECKING:
    from fsspec import AbstractFileSystem

    from .base import FileSystem


known_implementations = {
    Schemes.LOCAL: {"class": "mindvc.objects.fs.local.LocalFileSystem"},
    Schemes.MEMORY: {"class": "mindvc.objects.fs.memory.MemoryFileSystem"},
    Schemes.HDFS: {"class": "mindvc.objects.fs.hdfs.HDFSFileSystem"},
    Schemes.S3: {"class": "mindvc.objects.fs.s3.S3FileSystem"},
    Schemes.WEBHDFS: {"class": "mindvc.objects.fs.hdfs.WebHDFSFileSystem"},
    "git": {"class": "mindvc.objects.fs.git.GitFileSystem"},
}


def _import_class(cls: str):
    """Take a string FQP and return the imported class or identifier

    cls is of the form "package.module.klass".
    """
    import importlib

    mod, name = cls.rsplit(".", maxsplit=1)
    module = importlib.import_module(mod)
    return getattr(module, name)


class Registry(Mapping):
    def __init__(self, reg) -> None:
        self._registry = reg

    def __getitem__(self, key: str) -> Type["FileSystem"]:
        entry = self._registry.get(key) or self._registry[Schemes.LOCAL]
        try:
            return _import_class(entry["class"])
        except ImportError as exc:
            raise ImportError(entry["err"]) from exc

    def __iter__(self) -> Iterator[str]:
        yield from self._registry

    def __contains__(self, key: object) -> bool:
        return key in self._registry

    def __len__(self) -> int:
        return len(self._registry)


registry = Registry(known_implementations)


def get_fs_cls(remote_conf, cls=None, scheme=None):
    if cls:
        return _import_class(cls)

    if not scheme:
        scheme = urlparse(remote_conf["url"]).scheme
    return registry.get(scheme)


def as_filesystem(
    fs: "AbstractFileSystem",
    checksum: str = "md5",
    object_based: bool = False,
    **fs_args,
) -> "FileSystem":
    """
    Provides a way to transform any fsspec-based filesystems into a
    mindvc.objects.base.FileSystem compatible filesystem.

    This iterates through subclasses at first, and then creates an instance.
    If there's no existing subclass, it'll create a new subclass of
    the FileSystem and create a new instance out of it (the subclass will be
    reused, so please give attention to provided args, they should not change
    for the same filesystem).

    This is only intended for testing, please don't use it anywhere else.
    """
    from .base import FileSystem, ObjectFileSystem

    if isinstance(fs, FileSystem):
        return fs

    protos = (fs.protocol,) if isinstance(fs.protocol, str) else fs.protocol
    if "file" in protos:
        if klass := registry.get(Schemes.LOCAL):
            return klass()

    # if we have the class in our registry, instantiate with that.
    for proto in protos:
        if proto in registry:
            if klass := registry.get(proto):
                return klass(fs=fs, **fs_args)

    # fallback to unregistered subclasses
    for subclass in FileSystem.__subclasses__():
        for proto in protos:
            if proto == subclass.protocol:
                return subclass(fs=fs, **fs_args)

    # if that does not exist, create a new subclass and instantiate
    # from that (the subclass will be reused)
    fs_cls = ObjectFileSystem if object_based else FileSystem
    new_subclass = type(
        fs.__class__.__name__,
        (fs_cls,),
        {"PARAM_CHECKSUM": checksum, "protocol": protos[0]},
    )
    return new_subclass(fs=fs, **fs_args)
