"""Manages Git."""

import logging
import os
import re
from collections.abc import Mapping
from contextlib import contextmanager
from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    Iterable,
    Optional,
    Tuple,
    Type,
    Union,
)

from funcy import cached_property, first
from pathspec.patterns import GitWildMatchPattern

from ..base import Base
from ..exceptions import (
    FileNotInRepoError,
    GitHookAlreadyExists,
    RevError,
)
from ..utils import relpath

from .backend.base import BaseGitBackend, NoGitBackendError
from .backend.gitpython import GitPythonBackend
from .stash import Stash

if TYPE_CHECKING:
    from ..progress import GitProgressEvent

    from .objects import GitCommit, GitObject

    from .backend.base import SyncStatus

logger = logging.getLogger(__name__)

BackendCls = Type[BaseGitBackend]


class Git(Base):
    """Class for managing Git."""

    GITBACKENDS: Dict[str, BackendCls] = {
        "gitpython": GitPythonBackend,
    }
    GITIGNORE = ".gitignore"
    GIT_DIR = ".git"
    LOCAL_BRANCH_PREFIX = "refs/heads/"
    RE_HEXSHA = re.compile(r"^[0-9A-Fa-f]{4,40}$")
    BAD_REF_CHARS_RE = re.compile("[\177\\s~^:?*\\[]")

    def __init__(
        self, *args, backend: Optional[str] = None, **kwargs
    ):
        backend = backend or next(iter(self.GITBACKENDS))
        self.backend: BaseGitBackend = self.GITBACKENDS[backend](*args, **kwargs)
        super().__init__(self.backend.root_dir)

    @property
    def dir(self) -> str:
        return self.backend.dir

    @cached_property
    def hooks_dir(self):
        from pathlib import Path

        return Path(self.root_dir) / self.GIT_DIR / "hooks"

    @cached_property
    def stash(self) -> Stash:
        return Stash(self)

    @classmethod
    def clone(
        cls,
        url: str,
        to_path: str,
        rev: Optional[str] = None,
        **kwargs,
    ) -> "Git":
        for backend in cls.GITBACKENDS.values():
            try:
                backend.clone(url, to_path, **kwargs)
                repo = cls(to_path)
                if rev:
                    repo.checkout(rev)
                return repo
            except NotImplementedError:
                pass
        raise NoGitBackendError("clone")

    @classmethod
    def is_sha(cls, rev: Optional[str]) -> bool:
        return bool(rev and cls.RE_HEXSHA.search(rev))

    @classmethod
    def split_ref_pattern(cls, ref: str) -> Tuple[str, str]:
        name = cls.BAD_REF_CHARS_RE.split(ref, maxsplit=1)[0]
        return name, ref[len(name):]

    @staticmethod
    def _get_git_dir(root_dir: str) -> str:
        return os.path.join(root_dir, Git.GIT_DIR)

    @property
    def ignore_file(self):
        return self.GITIGNORE

    def _get_gitignore(self, path):
        ignore_file_dir = os.path.dirname(path)

        assert os.path.isabs(path)
        assert os.path.isabs(ignore_file_dir)

        entry = relpath(path, ignore_file_dir).replace(os.sep, "/")
        # NOTE: using '/' prefix to make path unambiguous
        if len(entry) > 0 and entry[0] != "/":
            entry = "/" + entry

        gitignore = os.path.join(ignore_file_dir, self.GITIGNORE)

        if not os.path.realpath(gitignore).startswith(self.root_dir + os.sep):
            raise FileNotInRepoError(
                f"'{path}' is outside of git repository '{self.root_dir}'"
            )

        return entry, gitignore

    def ignore(self, path: str) -> Optional[str]:
        entry, gitignore = self._get_gitignore(path)
        if self.is_ignored(path):
            return None

        self._add_entry_to_gitignore(entry, gitignore)
        return gitignore

    def _add_entry_to_gitignore(self, entry, gitignore):
        entry = GitWildMatchPattern.escape(entry)

        with open(gitignore, "a+", encoding="utf-8") as fobj:
            unique_lines = set(fobj.readlines())
            fobj.seek(0, os.SEEK_END)
            if fobj.tell() == 0:
                # Empty file
                prefix = ""
            else:
                fobj.seek(fobj.tell() - 1, os.SEEK_SET)
                last = fobj.read(1)
                prefix = "" if last == "\n" else "\n"
            new_entry = f"{prefix}{entry}\n"
            if new_entry not in unique_lines:
                fobj.write(new_entry)

    def ignore_remove(self, path: str) -> Optional[str]:
        entry, gitignore = self._get_gitignore(path)

        if not os.path.exists(gitignore):
            return None

        with open(gitignore, encoding="utf-8") as fobj:
            lines = fobj.readlines()

        filtered = list(filter(lambda x: x.strip() != entry.strip(), lines))

        if not filtered:
            os.unlink(gitignore)
            return None

        with open(gitignore, "w", encoding="utf-8") as fobj:
            fobj.writelines(filtered)
        return gitignore

    def verify_hook(self, name: str):
        if (self.hooks_dir / name).exists():
            raise GitHookAlreadyExists(name)

    def install_hook(
        self, name: str, script: str, interpreter: str = "sh"
    ):
        import shutil

        self.hooks_dir.mkdir(exist_ok=True)
        hook = self.hooks_dir / name

        directive = f"#!{shutil.which(interpreter) or '/bin/sh'}"
        hook.write_text(f"{directive}\n{script}\n", encoding="utf-8")
        hook.chmod(0o777)

    def install_merge_driver(
        self, name: str, description: str, driver: str
    ):
        self.backend.repo.git.config(f"merge.{name}.name", description)
        self.backend.repo.git.config(f"merge.{name}.driver", driver)

    def belongs_to_scm(self, path: str) -> bool:
        basename = os.path.basename(path)
        path_parts = os.path.normpath(path).split(os.path.sep)
        return basename == self.ignore_file or Git.GIT_DIR in path_parts

    def has_rev(self, rev: str) -> bool:
        try:
            self.resolve_rev(rev)
            return True
        except RevError:
            return False

    def close(self):
        self.backend.close()

    @property
    def no_commits(self):
        return not bool(self.get_ref("HEAD"))

    @classmethod
    def init(
        cls, path: str, bare: bool = False, _backend: Optional[str] = None
    ) -> "Git":
        for name, backend in cls.GITBACKENDS.items():
            if _backend and name != _backend:
                continue
            try:
                backend.init(path, bare=bare)
                # TODO: reuse created object instead of initializing a new one.
                return cls(path)
            except NotImplementedError:
                pass
        raise NoGitBackendError("init")

    def add_commit(
        self,
        paths: Union[str, Iterable[str]],
        message: str,
    ):
        self.add(paths)
        self.commit(msg=message)

    def is_ignored(self, path: str) -> bool:
        return self.backend.is_ignored(path)

    def add(self, paths: Union[str, Iterable[str]], update: bool = False):
        self.backend.add(paths, update=update)

    def commit(self, msg: str, no_verify: bool = False):
        self.backend.commit(msg, no_verify=no_verify)

    def checkout(
        self,
        branch: str,
        create_new: Optional[bool] = False,
        force: bool = False,
        **kwargs,
    ):
        self.backend.checkout(
            branch,
            create_new=create_new,
            force=force,
            **kwargs,
        )

    def fetch(
        self,
        remote: Optional[str] = None,
        force: bool = False,
        unshallow: bool = False,
    ):
        self.backend.fetch(
            remote=remote,
            force=force,
            unshallow=unshallow,
        )

    def pull(self, **kwargs):
        self.backend.pull(**kwargs)

    def push(self):
        self.backend.push()

    def branch(self, branch: str):
        self.backend.branch(branch)

    def tag(self, tag: str):
        self.backend.tag(tag)

    def untracked_files(self) -> Iterable[str]:
        return self.backend.untracked_files()

    def is_tracked(self, path: str) -> bool:
        return self.backend.is_tracked(path)

    def is_dirty(self, untracked_files: bool = False) -> bool:
        return self.backend.is_dirty(untracked_files=untracked_files)

    def active_branch(self) -> str:
        return self.backend.active_branch()

    def list_branches(self) -> Iterable[str]:
        return self.backend.list_branches()

    def list_tags(self) -> Iterable[str]:
        return self.backend.list_tags()

    def list_all_commits(self) -> Iterable[str]:
        return self.backend.list_all_commits()

    def get_rev(self) -> str:
        return self.backend.get_rev()

    def resolve_rev(self, rev: str) -> str:
        return self.backend.resolve_rev(rev)

    def resolve_commit(self, rev: str) -> "GitCommit":
        return self.backend.resolve_commit(rev)

    def set_ref(
        self,
        name: str,
        new_ref: str,
        old_ref: Optional[str] = None,
        message: Optional[str] = None,
        symbolic: Optional[bool] = False,
    ):
        self.backend.set_ref(
            name,
            new_ref,
            old_ref=old_ref,
            message=message,
            symbolic=symbolic,
        )

    def get_ref(self, name: str, follow: bool = True) -> Optional[str]:
        return self.backend.get_ref(name, follow=follow)

    def remove_ref(self, name: str, old_ref: Optional[str] = None):
        self.backend.remove_ref(name, old_ref=old_ref)

    def iter_refs(self, base: Optional[str] = None) -> Iterable[str]:
        return self.backend.iter_refs(base=base)

    def iter_remote_refs(self, url: str, base: Optional[str] = None, **kwargs) -> Iterable[str]:
        return self.backend.iter_remote_refs(url, base=base, **kwargs)

    def get_refs_containing(self, rev: str, pattern: Optional[str] = None) -> Iterable[str]:
        return self.backend.get_refs_containing(rev, pattern=pattern)

    def push_refspecs(
        self,
        url: str,
        refspecs: Union[str, Iterable[str]],
        force: bool = False,
        on_diverged: Optional[Callable[[str, str], bool]] = None,
        progress: Callable[["GitProgressEvent"], None] = None,
        **kwargs,
    ) -> Mapping[str, SyncStatus]:
        return self.backend.push_refspecs(
            url,
            refspecs,
            force=force,
            on_diverged=on_diverged,
            progress=progress,
            **kwargs,
        )

    def fetch_refspecs(
        self,
        url: str,
        refspecs: Union[str, Iterable[str]],
        force: bool = False,
        on_diverged: Optional[Callable[[str, str], bool]] = None,
        progress: Callable[["GitProgressEvent"], None] = None,
        **kwargs,
    ) -> Mapping[str, SyncStatus]:
        return self.backend.fetch_refspecs(
            url,
            refspecs,
            force=force,
            on_diverged=on_diverged,
            progress=progress,
            **kwargs,
        )

    def _stash_iter(self, ref: str):
        return self.backend._stash_iter(ref)

    def _stash_push(
        self,
        ref: str,
        message: Optional[str] = None,
        include_untracked: Optional[bool] = False,
    ) -> Tuple[Optional[str], bool]:
        return self.backend._stash_push(
            ref,
            message=message,
            include_untracked=include_untracked,
        )

    def _stash_apply(
        self,
        rev: str,
        reinstate_index: bool = False,
        skip_conflicts: bool = False,
        **kwargs,
    ):
        self.backend._stash_apply(
            rev,
            reinstate_index=reinstate_index,
            skip_conflicts=skip_conflicts,
            **kwargs,
        )

    def _stash_drop(self, ref: str, index: int):
        self.backend._stash_drop(ref, index)

    def _describe(
        self,
        revs: Iterable[str],
        base: Optional[str] = None,
        match: Optional[str] = None,
        exclude: Optional[str] = None,
    ) -> Mapping[str, Optional[str]]:
        return self.backend._describe(
            revs,
            base=base,
            match=match,
            exclude=exclude,
        )

    def diff(self, rev_a: str, rev_b: str, binary=False) -> str:
        return self.backend.diff(rev_a, rev_b, binary=binary)

    def reset(self, hard: bool = False, paths: Iterable[str] = None):
        self.backend.reset(hard=hard, paths=paths)

    def checkout_index(
        self,
        paths: Optional[Iterable[str]] = None,
        force: bool = False,
        ours: bool = False,
        theirs: bool = False,
    ):
        self.backend.checkout_index(
            paths=paths,
            force=force,
            ours=ours,
            theirs=theirs,
        )

    def status(
        self, ignored: bool = False, untracked_files: str = "all"
    ) -> Tuple[Mapping[str, Iterable[str]], Iterable[str], Iterable[str]]:
        return self.backend.status(
            ignored=ignored, untracked_files=untracked_files
        )

    def merge(
        self,
        rev: str,
        commit: bool = True,
        msg: Optional[str] = None,
        squash: bool = False,
    ) -> Optional[str]:
        return self.backend.merge(
            rev,
            commit=commit,
            msg=msg,
            squash=squash,
        )

    def validate_git_remote(self, url: str, **kwargs):
        self.backend.validate_git_remote(url, **kwargs)

    def get_tree_obj(self, rev: str, **kwargs) -> GitObject:
        return self.backend.get_tree_obj(rev, **kwargs)

    def branch_revs(
        self, branch: str, end_rev: Optional[str] = None
    ) -> Iterable[str]:
        """Iterate over revisions in a given branch (from newest to oldest).

        If end_rev is set, iterator will stop when the specified revision is
        reached.
        """
        commit = self.resolve_commit(branch)
        while commit is not None:
            yield commit.hexsha
            parent = first(commit.parents)
            if parent is None or parent == end_rev:
                return
            commit = self.resolve_commit(parent)

    @contextmanager
    def detach_head(
        self,
        rev: Optional[str] = None,
        force: bool = False,
        client: str = "scm",
    ):
        """Context manager for performing detached HEAD SCM operations.

        Detaches and restores HEAD similar to interactive git rebase.
        Restore is equivalent to 'reset --soft', meaning the caller is
        is responsible for preserving & restoring working tree state
        (i.e. via stash) when applicable.

        Yields revision of detached head.
        """
        if not rev:
            rev = "HEAD"
        orig_head = self.get_ref("HEAD", follow=False)
        logger.debug("Detaching HEAD at '%s'", rev)
        self.checkout(rev, detach=True, force=force)
        try:
            yield self.get_ref("HEAD")
        finally:
            prefix = self.LOCAL_BRANCH_PREFIX
            if orig_head.startswith(prefix):
                symbolic = True
                name = orig_head[len(prefix) :]
            else:
                symbolic = False
                name = orig_head
            self.set_ref(
                "HEAD",
                orig_head,
                symbolic=symbolic,
                message=f"{client}: Restore HEAD to '{name}'",
            )
            logger.debug("Restore HEAD to '%s'", name)
            self.reset()

    @contextmanager
    def stash_workspace(self, **kwargs):
        """Stash and restore any workspace changes.

        Yields revision of the stash commit. Yields None if there were no
        changes to stash.
        """
        logger.debug("Stashing workspace")
        rev = self.stash.push(**kwargs)
        try:
            yield rev
        finally:
            if rev:
                logger.debug("Restoring stashed workspace")
                self.stash.pop()

    def _reset(self):
        self.backend._reset()

    def describe(
        self,
        revs: Iterable[str],
        base: Optional[str] = None,
        match: Optional[str] = None,
        exclude: Optional[str] = None,
    ) -> Dict[str, Optional[str]]:
        results: Dict[str, Optional[str]] = {}
        remained_revs = set()
        if base == "refs/heads":
            current_rev = self.get_rev()
            head_ref = self.get_ref("HEAD", follow=False)
            for rev in revs:
                if current_rev == rev and head_ref.startswith(base):
                    results[rev] = self.get_ref("HEAD", follow=False)
                else:
                    remained_revs.add(rev)
        else:
            remained_revs = set(revs)
        if remained_revs:
            results.update(self._describe(remained_revs, base, match, exclude))
        return results
