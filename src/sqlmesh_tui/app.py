import asyncio
import concurrent.futures
import contextlib
import logging
import re
import shlex
import typing as t
from collections import deque

import textual.binding
import textual.containers as container
import textual.widgets as widget
from sqlmesh import Context
from sqlmesh.core.console import TerminalConsole
from textual import work
from textual.app import App, ComposeResult
from textual.logging import TextualHandler
from textual.reactive import reactive  # noqa: F401

logging.basicConfig(
    level="NOTSET",
    handlers=[TextualHandler()],
)


class EnvironmentRadioSet(widget.RadioSet):
    """An improved RadioSet that works better with filtered items"""

    DEFAULT_CSS = widget.RadioSet.DEFAULT_CSS.replace("RadioSet", "EnvironmentRadioSet")
    BINDINGS: t.ClassVar[t.List[textual.binding.BindingType]] = [
        textual.binding.Binding("down,right,j,l", "next_button", "", show=False),
        textual.binding.Binding("enter,space", "toggle", "Toggle", show=False),
        textual.binding.Binding("up,left,k,h", "previous_button", "", show=False),
    ]

    def action_next_button(self) -> None:
        """Cycle to next item.

        Ensures a noop if all nodes are filtered, skip disabled nodes when key scrolling
        """
        if all(node.disabled for node in self._nodes):
            return
        super().action_next_button()
        while self._selected is not None and self._nodes[self._selected].disabled:
            super().action_next_button()
        assert self._selected is not None
        self._nodes[self._selected].scroll_visible(force=True)

    def action_previous_button(self) -> None:
        """Cycle to next item.

        Ensures a noop if all nodes are filtered, skip disabled nodes when key scrolling
        """
        if all(node.disabled for node in self._nodes):
            return
        super().action_previous_button()
        while self._selected is not None and self._nodes[self._selected].disabled:
            super().action_previous_button()
        assert self._selected is not None
        self._nodes[self._selected].scroll_visible(force=True)

    def action_toggle(self) -> None:
        """Toggle the state of the currently-selected button.

        Ensure we cannot toggle a disabled node
        """
        if self._nodes:
            button = self._nodes[self._selected or 0]
            assert isinstance(button, widget.RadioButton)
            if not button.disabled:
                button.toggle()

    @property
    def active_environment(self) -> str:
        """Get the name of the active environment."""
        if not self.pressed_button:
            return "prod"
        return self.pressed_button.label.plain


class InteractiveTerminal(widget.Static):
    """An interactive terminal widget for SQLMesh"""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._prompt_return = "__AWAITING_INPUT__"
        self._in_prompt = False
        self.border_title = "REPL"
        self._history = deque(maxlen=100)

    @property
    def app_log(self) -> widget.RichLog:
        """Get the app's rich log."""
        return self.query_one(widget.RichLog)

    @property
    def app_tty(self) -> widget.Input:
        """Get the app's tty input."""
        return self.query_one(widget.Input)

    def compose(self) -> ComposeResult:
        """Create child widgets for the component."""
        yield widget.RichLog(markup=True, wrap=True)
        yield widget.Input(placeholder="Enter a command...")

    def as_sqlmesh_console(self) -> TerminalConsole:
        """Get a SQLMesh console implementation for this widget."""
        return type(
            "TerminalConsole",
            (TerminalConsole,),
            {
                "_print": self._print,
                "_confirm": self._confirm,
                "_prompt": self._prompt,
            },
        )()

    @contextlib.contextmanager
    def _interact(self, message: str, placeholder: str):
        """Context manager for prompts.

        Focuses the tty input and sets the placeholder.
        """
        self.app_log.write(message)
        self._in_prompt = True
        self._prompt_return = "__AWAITING_INPUT__"
        self.app_tty.focus()
        self.app_tty.value = ""
        orig_placeholder = self.app_tty.placeholder
        self.app_tty.placeholder = placeholder
        yield
        self._in_prompt = False
        self.app_tty.placeholder = orig_placeholder

    def _print(self, value: t.Any, **kwargs: t.Any) -> None:
        """Used for SQLMesh as widget-integrated Console implementation"""
        self.app_log.write(value)

    def _confirm(self, message: str, **kwargs: t.Any) -> bool:
        """Used for SQLMesh as widget-integrated Console implementation"""

        async def confirm():
            while self._prompt_return == "__AWAITING_INPUT__":
                await asyncio.sleep(0.25)
            if self._prompt_return in ("y", "yes"):
                return True
            elif self._prompt_return in ("n", "no"):
                return False
            else:
                self.app_log.write(
                    f"[red]Invalid response: {self._prompt_return}[/red]"
                )
                return False

        with self._interact(message, "y/n"):
            return (
                concurrent.futures.ThreadPoolExecutor(max_workers=2)
                .submit(asyncio.run, confirm())
                .result()
            )

    def _prompt(self, message: str, **kwargs: t.Any) -> str:
        """Used for SQLMesh as widget-integrated Console implementation"""

        async def prompt():
            while self._prompt_return == "__AWAITING_INPUT__":
                await asyncio.sleep(0.25)
            self._in_prompt = False
            return self._prompt_return

        with self._interact(message, "Enter a value"):
            return (
                concurrent.futures.ThreadPoolExecutor(max_workers=2)
                .submit(asyncio.run, prompt())
                .result()
            )

    async def on_click(self, event) -> None:
        """Handle clicks in the tty input."""
        self.app_tty.focus()

    async def key_up(self) -> None:
        if len(self._history) > 0:
            self.app_tty.value = self._history[0]
            self._history.rotate(-1)

    async def key_down(self) -> None:
        if len(self._history) > 0:
            self._history.rotate(1)
            self.app_tty.value = self._history[0]

    async def key_enter(self) -> None:
        """Handle enter keypresses in the tty input."""
        capture = self.app_tty.value
        app = t.cast(SQLMeshApp, self.app)
        try:
            self.app_log.write(f"\n[blue]> {capture}[/blue]")
            self.app_tty.value = ""
            # if capture startswith ! then forward it to shell
            if self._in_prompt:
                self._prompt_return = capture
            elif capture == "":
                return
            elif capture.startswith(":"):
                # Ban certain commands
                if capture == ":":
                    cmds = [
                        func
                        for func in dir(app.ctx)
                        if not func.startswith("_") and callable(getattr(app.ctx, func))
                    ]
                    self.app_log.write(
                        "[b]Context Methods:[/b]\n- " + "\n- ".join(cmds)
                    )
                    return

                args = shlex.split(capture[1:])
                command = args.pop(0)
                if command in ("plan",):
                    self.app_log.write(
                        f"[red]:{command} context command is not supported, try using a builtin instead[/red]"
                    )
                    return
                kwargs = {}
                posargs = []
                for arg in args:
                    if re.match(r"^[a-zA-Z0-9_]+=", arg):
                        key, value = arg.split("=", 1)
                        try:
                            kwargs[key] = eval(value)
                        except Exception:
                            kwargs[key] = value.strip('"').strip("'")
                    else:
                        try:
                            posargs.append(eval(arg))
                        except Exception:
                            posargs.append(arg.strip('"').strip("'"))
                try:
                    rv = getattr(app.ctx, command)(*posargs, **kwargs)
                    if rv is not None:
                        self.app_log.write(repr(rv))
                except Exception as e:
                    self.app_log.write(repr(e))
                    self.app_log.write(f"[red]Error running command: {capture}[/red]")
                    raise
            elif capture.startswith("!"):
                if capture == "!":
                    self.app_log.write(
                        "[red]Bang command should not be empty, ensure you follow with a shell command[/red]"
                    )
                proc = await asyncio.create_subprocess_shell(
                    capture[1:],
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                try:
                    await asyncio.wait_for(proc.wait(), 10.0)
                except asyncio.exceptions.TimeoutError:
                    proc.kill()
                    self.notify(
                        f"Command {capture} timed out after 10 seconds... Long running commands should be ran in a dedicated shell",
                        severity="error",
                    )
                except Exception:
                    proc.kill()
                    self.notify(f"Command {capture} failed...", severity="error")
                if proc.stdout:
                    stdout = await proc.stdout.read()
                    if stdout:
                        self.app_log.write(stdout.decode())
                if proc.stderr:
                    stderr = await proc.stderr.read()
                    if stderr:
                        self.app_log.write(f"\n[red]Stderr:\n{stderr.decode()}[/red]")
            elif capture == "?":
                # These mirror ctrl+<key> commands
                self.app_log.write("[b]Commands:[/b]")
                self.app_log.write("[b]a:[/b] Run audits")
                self.app_log.write("[b]c:[/b] Check status")
                self.app_log.write("[b]d:[/b] Diff env to prod")
                self.app_log.write("[b]p:[/b] Plan")
                self.app_log.write("[b]p <env>:[/b] Plan for a specific environment")
                self.app_log.write("[b]f:[/b] Fetch environments")
                self.app_log.write("[b]r:[/b] Run intervals")
                self.app_log.write("[b]r <env>:[/b] Run intervals for specific env")
                self.app_log.write("[b]t:[/b] Run unit tests")
                self.app_log.write("[b]q:[/b] Quit")
                self.app_log.write("[b]!<cmd>:[/b] Run a shell command")
                self.app_log.write(
                    "[b]:<cmd>:[/b] Run a method on the sqlmesh context object"
                )
                self.app_log.write("[b]?:[/b] Show this help")
            elif capture in ("q", "quit"):
                app.exit()
            elif capture in ("a", "audit"):
                asyncio.create_task(app.action_run_audits())
            elif capture in ("c", "check"):
                await app.action_check_status()
            elif capture in ("d", "diff"):
                await app.action_get_diff()
            elif capture in ("p", "plan"):
                _ = await app.action_run_plan(app.active_environment)
            elif capture.startswith("p ") or capture.startswith("plan "):
                env_name = capture.split(" ", 1)[1].strip()
                _ = await app.action_run_plan(env_name)
            elif capture in ("f", "fetch"):
                await app.action_fetch_environments()
            elif capture in ("t", "test"):
                await app.action_run_unit_tests()
            elif capture in ("r", "run"):
                _ = await app.action_run_intervals(app.active_environment)
            elif capture.startswith("r ") or capture.startswith("run "):
                env_name = capture.split(" ", 1)[1].strip()
                _ = await app.action_run_intervals(env_name)
            else:
                self.app_log.write(f"[red]Unknown command: {capture}[/red]")
                raise NotImplementedError(capture)
        except Exception:
            app.bell()
            self.app_tty.value = capture
        finally:
            if not self._in_prompt:
                self._history.appendleft(capture)
            self.app_tty.value = ""


class SQLMeshApp(App[None]):
    """A Textual app to manage SQLMesh projects."""

    CSS_PATH = "app.tcss"
    BINDINGS = [
        ("D", "toggle_dark", "Toggle dark mode"),
        ("a", "run_audits", "Run audits"),
        ("f", "fetch_environments", "Fetch environments from state store"),
        ("c", "check_status", "Check connection status"),
        ("d", "get_diff", "Get context diff vs prod"),
        ("r", "run_intervals(None)", "Run intervals for active env"),
        ("R", "run_intervals('prod')", "Run intervals for prod"),
        ("t", "run_unit_tests", "Run unit tests"),
        ("p", "run_plan(None)", "Run plan against active env"),
        ("P", "run_plan('prod')", "Run plan against prod"),
        ("q", "quit", "Quit app"),
    ]
    TITLE = "SQLMesh TUI"
    SUB_TITLE = "A text user interface for managing a SQLMesh project"

    def __init__(self, ctx: Context, *args, **kwargs) -> None:
        self.ctx = ctx
        super().__init__(*args, **kwargs)

    @property
    def terminal(self) -> InteractiveTerminal:
        """Get the app's terminal."""
        return self.query_one(InteractiveTerminal)

    @property
    def app_log(self) -> widget.RichLog:
        """Get the app's rich log."""
        return self.query_one(InteractiveTerminal).app_log

    @property
    def app_tty(self) -> widget.Input:
        """Get the app's tty input."""
        return self.query_one(InteractiveTerminal).app_tty

    @property
    def environment_search(self) -> widget.Input:
        """Get the environment search input."""
        return self.query_one("#environment_search", widget.Input)

    @property
    def environment_picker(self) -> EnvironmentRadioSet:
        """Get the environment picker radio set."""
        return self.query_one("#environment_picker", EnvironmentRadioSet)

    @property
    def active_environment(self) -> str:
        """Get the name of the active environment."""
        return self.environment_picker.active_environment

    @property
    def snapshot_viewer(self) -> container.ScrollableContainer:
        """Get the snapshot viewer container."""
        return self.query_one("#snapshot_viewer", container.ScrollableContainer)

    def write_to_log(self, msg: str, **kwargs) -> None:
        """Write a message to the user-facing rich log."""
        self.app_log.write(msg, **kwargs)

    @work(exclusive=True)
    async def sqlmesh_fetch_environments(self) -> list:
        """Load environments from state and update self.environment_picker radioset."""
        self.environment_search.value = ""
        envs = await asyncio.to_thread(self.ctx.state_sync.get_environments)
        buttons = [
            widget.RadioButton(e.name, name="env_switch") for _, e in enumerate(envs)
        ]
        await self.environment_picker.remove_children()
        await self.environment_picker.mount(*buttons)
        self.environment_picker.action_toggle()
        self.environment_picker.focus()
        return envs

    @work(thread=True)
    def sqlmesh_status(self) -> None:
        """Get the status of the context and write to log."""
        self.ctx.print_info()

    @work(thread=True, exclusive=True)
    def sqlmesh_audit(self, start: str = "1 month ago", end: str = "today") -> None:
        """Run all of the audits"""
        # TODO: use the snapshot UI interface to select specific models to audit?
        try:
            self.ctx.audit(start=start, end=end)
        except Exception as e:
            self.notify(repr(e), severity="error")

    @work(thread=True)
    def sqlmesh_diff(self) -> None:
        """Diff the active environment to prod and write to log."""
        self.ctx.diff(self.active_environment)

    @work(thread=True, exclusive=True)
    def sqlmesh_plan(self, name: t.Optional[str] = None, /, **kwargs) -> None:
        """Create a plan and optionally apply it"""
        try:
            self.ctx.plan(name or self.active_environment, include_unmodified=True)
        except Exception as e:
            self.notify(repr(e), severity="error")
        else:
            _ = self.sqlmesh_fetch_environments().run()

    @work(thread=True)
    def sqlmesh_test(self) -> None:
        """Run unit tests"""
        # TODO: reimplement the underlying method for more customization
        try:
            self.ctx._run_plan_tests()
        except Exception:
            pass

    @work(thread=True)
    def sqlmesh_run(self, name: t.Optional[str] = None) -> None:
        """Run model intervals"""
        env = name or self.active_environment
        try:
            self.ctx.run(env)
        except Exception:
            self.notify("Run failed for `{}`".format(env), severity="error")
        else:
            self.notify(
                "Run succeeded for `{}`".format(env),
            )

    async def on_mount(self) -> None:
        """On mount, load environments from state.

        Also overrides context.console with an interactive widget terminal.
        """
        self.write_to_log("[b]Welcome to LazySQLMesh![/b]\n")
        if not isinstance(self.ctx.console, InteractiveTerminal):
            self.ctx.console = self.terminal.as_sqlmesh_console()
        envs = await self.sqlmesh_fetch_environments().wait()
        self.notify(f"Loaded {len(envs)} environments from state")
        await self.sqlmesh_status().wait()
        self.app_tty.focus()

    def compose(self) -> ComposeResult:
        """Create child widgets for the app."""
        yield widget.Header(show_clock=True)
        sidebar = container.Container(
            widget.Input(placeholder="Search...", id="environment_search"),
            container.ScrollableContainer(
                EnvironmentRadioSet(id="environment_picker"),
            ),
            container.Horizontal(
                widget.Button("Create", disabled=True),
                widget.Button("Compare", id="compare"),
                widget.Button("Delete", variant="error"),
                id="environment_controls",
            ),
            id="sidebar",
        )
        sidebar.border_title = "Environments"
        yield sidebar
        # plan should ostensibly be its own screen
        yield container.Container(id="plan_manager")
        snapshot_viewer = container.ScrollableContainer(id="snapshot_viewer")
        snapshot_viewer.border_title = "Snapshots"
        yield snapshot_viewer
        yield InteractiveTerminal(id="tty")
        yield widget.Footer()

    async def on_input_changed(self, event: widget.Input.Changed) -> None:
        if event.input.id == "environment_search":
            q = self.environment_search.value
            for btn in self.environment_picker.walk_children(widget.RadioButton):
                if (not q) or q.lower() in btn.label.plain.lower():
                    btn.disabled = False
                else:
                    btn.disabled = True

    async def on_button_pressed(self, event: widget.Button.Pressed) -> None:
        if event.button.id == "compare":
            _ = self.sqlmesh_diff().run()

    async def on_radio_set_changed(self, event: widget.RadioSet.Changed) -> None:
        """Change environment when a radio button is changed."""
        if event.radio_set.id == "environment_picker":
            self.notify(f"Loading {event.pressed.label.plain}")
            env = self.ctx.state_sync.get_environment(event.pressed.label.plain)
            assert env
            await self.snapshot_viewer.remove_children()
            # TODO: lets make this better
            table = widget.DataTable(cursor_type="row")
            table.add_columns("Snapshot Name", "ID")
            table.add_rows([[s.name, s.snapshot_id.identifier] for s in env.snapshots])
            await self.snapshot_viewer.mount(table)

    async def action_toggle_dark(self) -> None:
        """An action to toggle dark mode."""
        self.dark = not self.dark

    async def action_run_audits(
        self, start: str = "1 month ago", end: str = "today"
    ) -> None:
        """An action to run audits"""
        await self.sqlmesh_audit(start=start, end=end).wait()

    async def action_fetch_environments(self) -> None:
        """An action to refresh the environment list."""
        await self.sqlmesh_fetch_environments().wait()
        self.app_log.write("[green]Fetched environments[/green]")

    async def action_check_status(self) -> None:
        """An action to check the statuc"""
        await self.sqlmesh_status().wait()

    async def action_get_diff(self) -> None:
        """An action to get the context diff"""
        await self.sqlmesh_diff().wait()

    async def action_run_unit_tests(self) -> None:
        """An action to run all unit tests"""
        await self.sqlmesh_test().wait()

    async def action_run_plan(self, name: str, **kwargs) -> None:
        """An action to create and optionally apply a plan"""
        self.app_tty.focus()
        _ = self.sqlmesh_plan(name, **kwargs).run()

    async def action_run_intervals(self, name: str) -> None:
        """An action to run model intervals"""
        await self.sqlmesh_run(name).wait()
        await self.sqlmesh_fetch_environments().wait()


if __name__ == "__main__":
    app = SQLMeshApp(
        Context(paths=["tests/fixtures/project"]),
    )  # type: ignore
    app.run()
