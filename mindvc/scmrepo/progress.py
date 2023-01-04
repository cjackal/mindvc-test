from typing import NamedTuple, Optional, Union

from funcy import compose


def code2desc(op_code):
    from git import RootUpdateProgress as OP

    ops = {
        OP.COUNTING: "Counting",
        OP.COMPRESSING: "Compressing",
        OP.WRITING: "Writing",
        OP.RECEIVING: "Receiving",
        OP.RESOLVING: "Resolving",
        OP.FINDING_SOURCES: "Finding sources",
        OP.CHECKING_OUT: "Checking out",
        OP.CLONE: "Cloning",
        OP.FETCH: "Fetching",
        OP.UPDWKTREE: "Updating working tree",
        OP.REMOVE: "Removing",
        OP.PATHCHANGE: "Changing path",
        OP.URLCHANGE: "Changing URL",
        OP.BRANCHCHANGE: "Changing branch",
    }
    return ops.get(op_code & OP.OP_MASK, "")


class GitProgressEvent(NamedTuple):
    phase: str = ""
    completed: Optional[int] = None
    total: Optional[int] = None
    message: str = ""

    @classmethod
    def parsed_from_gitpython(
        cls,
        op_code,
        cur_count,
        max_count=None,
        message="",  # pylint: disable=redefined-outer-name
    ):
        return cls(code2desc(op_code), cur_count, max_count, message)


class GitProgressReporter:
    def __init__(self, fn) -> None:
        from git.util import CallableRemoteProgress

        self._reporter = CallableRemoteProgress(self.wrap_fn(fn))

    def __call__(self, msg: Union[str, bytes]) -> None:
        self._reporter._parse_progress_line(
            msg.decode("utf-8", errors="replace").strip()
            if isinstance(msg, bytes)
            else msg
        )

    @staticmethod
    def wrap_fn(fn):
        return compose(fn, GitProgressEvent.parsed_from_gitpython)
