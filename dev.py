import argparse
import subprocess
import sys
import textwrap
from glob import iglob
from subprocess import CalledProcessError
from typing import Callable


def run(command: str, **kwargs):
    if "shell" not in kwargs:
        kwargs["shell"] = True
    if "check" not in kwargs:
        kwargs["check"] = True

    message = f'"{command}"'
    cwd = kwargs.get("cwd", None)
    if cwd:
        message += f' in "{cwd}"'
    print(f"RUNNING: {message}")

    try:
        subprocess.run(command, **kwargs)
        print(f"DONE: {message}")
    except KeyboardInterrupt:
        pass
    except CalledProcessError:
        print(f"FAILED: {message}", file=sys.stderr)
        exit(1)


def config_vscode():
    run("mkdir -p .vscode && cp -r templates/.vscode/* .vscode/")


def sync():
    run("uv sync --all-packages")


PACKAGES_PATTERN = "packages/*"


def deptry():
    for path in iglob(PACKAGES_PATTERN):
        run("deptry .", cwd=path)


def ruff():
    for path in iglob(PACKAGES_PATTERN):
        run("ruff check --fix . && ruff format .", cwd=path)


def pyright():
    for path in iglob(PACKAGES_PATTERN):
        run("pyright", cwd=path)


def pytest():
    for path in iglob(PACKAGES_PATTERN):
        run("pytest", cwd=path)


def check():
    ruff()
    pyright()
    deptry()
    pytest()


class Args(argparse.Namespace):
    def __init__(self):
        self.handler: Callable[[], None] | None = None


def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        description=textwrap.dedent("""\
            개발용 유틸리티 스크립트입니다.
        """),
    )
    subparsers = parser.add_subparsers()

    subparsers.add_parser(
        "config-vscode",
        help="프로젝트 VS Code 구성을 템플릿으로 덮어씁니다. 주의해주세요.",
    ).set_defaults(handler=config_vscode)

    subparsers.add_parser(
        "sync", help='"uv sync --all-packages"를 호출합니다.'
    ).set_defaults(handler=sync)

    subparsers.add_parser(
        "deptry", help='모든 하위 패키지 경로에서 "deptry"를 실행합니다.'
    ).set_defaults(handler=deptry)

    subparsers.add_parser(
        "ruff",
        help='모든 하위 패키지 경로에서 "ruff check --fix"와 "ruff format"을 실행합니다.',
    ).set_defaults(handler=ruff)

    subparsers.add_parser(
        "pyright", help='모든 하위 패키지 경로에서 "pyright"를 실행합니다.'
    ).set_defaults(handler=pyright)

    subparsers.add_parser(
        "pytest", help='모든 하위 패키지 경로에서 "pytest"를 실행합니다.'
    ).set_defaults(handler=pytest)

    subparsers.add_parser(
        "check",
        help='모든 하위 패키지 경로에서 "ruff check", "pyright", "deptry", "pytest"를 실행합니다.',
    ).set_defaults(handler=check)

    args = parser.parse_args(namespace=Args())
    if not args.handler:
        parser.print_help()
        exit(1)

    args.handler()


if __name__ == "__main__":
    main()
