from __future__ import annotations

import queue
import subprocess
import sys
import threading
from dataclasses import dataclass, field


def run_desktop_shell() -> None:
    try:
        import tkinter as tk
        from tkinter import ttk
    except Exception as exc:
        raise RuntimeError("Tkinter is not available in this Python runtime.") from exc

    @dataclass
    class ShellState:
        root: tk.Tk
        logs: tk.Text
        queue: queue.Queue[str] = field(default_factory=queue.Queue)
        agent_process: subprocess.Popen[str] | None = None

        def write_log(self, line: str) -> None:
            self.queue.put(line.rstrip("\n"))

        def poll_logs(self) -> None:
            while True:
                try:
                    line = self.queue.get_nowait()
                except queue.Empty:
                    break
                self.logs.configure(state=tk.NORMAL)
                self.logs.insert(tk.END, line + "\n")
                self.logs.see(tk.END)
                self.logs.configure(state=tk.DISABLED)
            self.root.after(100, self.poll_logs)

        def run_cli(self, *args: str, keep_process: bool = False) -> None:
            cmd = [sys.executable, "-m", "pcca.cli", *args]
            self.write_log("$ " + " ".join(cmd))
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            if keep_process:
                self.agent_process = proc

            def _reader() -> None:
                assert proc.stdout is not None
                for line in proc.stdout:
                    self.write_log(line)
                proc.wait()
                self.write_log(f"[exit {proc.returncode}] {' '.join(args)}")
                if self.agent_process is proc:
                    self.agent_process = None

            threading.Thread(target=_reader, daemon=True).start()

        def stop_agent(self) -> None:
            if self.agent_process is None:
                self.write_log("No running agent process.")
                return
            self.agent_process.terminate()
            self.write_log("Sent terminate signal to running agent.")

    root = tk.Tk()
    root.title("PCCA Desktop Shell")
    root.geometry("1040x760")

    notebook = ttk.Notebook(root)
    notebook.pack(fill=tk.BOTH, expand=True)

    onboarding_tab = ttk.Frame(notebook, padding=12)
    subjects_tab = ttk.Frame(notebook, padding=12)
    sources_tab = ttk.Frame(notebook, padding=12)
    prefs_tab = ttk.Frame(notebook, padding=12)
    actions_tab = ttk.Frame(notebook, padding=12)
    logs_tab = ttk.Frame(notebook, padding=12)

    notebook.add(onboarding_tab, text="Onboarding")
    notebook.add(subjects_tab, text="Subjects")
    notebook.add(sources_tab, text="Sources")
    notebook.add(prefs_tab, text="Preferences")
    notebook.add(actions_tab, text="Actions")
    notebook.add(logs_tab, text="Logs")

    logs_widget = tk.Text(logs_tab, wrap=tk.WORD, state=tk.DISABLED, height=35)
    logs_widget.pack(fill=tk.BOTH, expand=True)

    state = ShellState(root=root, logs=logs_widget)

    # Onboarding tab
    ttk.Label(
        onboarding_tab,
        text=(
            "Typical first-run flow:\n"
            "1) Init DB\n"
            "2) Create subject(s)\n"
            "3) Add source URLs or connect account follows\n"
            "4) Start agent\n"
            "5) In Telegram use /read_content and /get_digest for on-demand tests"
        ),
        justify=tk.LEFT,
    ).pack(anchor="w", pady=(0, 12))

    controls_row = ttk.Frame(onboarding_tab)
    controls_row.pack(fill=tk.X, pady=(0, 12))
    ttk.Button(controls_row, text="Init DB", command=lambda: state.run_cli("init-db")).pack(side=tk.LEFT, padx=4)
    ttk.Button(
        controls_row,
        text="Start Agent",
        command=lambda: state.run_cli("run-agent", keep_process=True),
    ).pack(side=tk.LEFT, padx=4)
    ttk.Button(controls_row, text="Stop Agent", command=state.stop_agent).pack(side=tk.LEFT, padx=4)
    ttk.Button(
        controls_row,
        text="Run Read Content Now",
        command=lambda: state.run_cli("run-nightly-once"),
    ).pack(side=tk.LEFT, padx=4)
    ttk.Button(
        controls_row,
        text="Run Get Digest Now",
        command=lambda: state.run_cli("run-digest-once"),
    ).pack(side=tk.LEFT, padx=4)

    # Subjects tab
    subject_name_var = tk.StringVar()
    ttk.Label(subjects_tab, text="Subject name").grid(row=0, column=0, sticky="w")
    ttk.Entry(subjects_tab, textvariable=subject_name_var, width=50).grid(row=0, column=1, sticky="ew", padx=6)
    ttk.Button(
        subjects_tab,
        text="Create Subject",
        command=lambda: state.run_cli("create-subject", "--name", subject_name_var.get().strip()),
    ).grid(row=0, column=2, padx=6)
    ttk.Button(subjects_tab, text="List Subjects", command=lambda: state.run_cli("list-subjects")).grid(row=0, column=3, padx=6)
    subjects_tab.columnconfigure(1, weight=1)

    # Sources tab
    source_subject_var = tk.StringVar()
    source_platform_var = tk.StringVar(value="x")
    source_id_var = tk.StringVar()
    source_url_var = tk.StringVar()

    ttk.Label(sources_tab, text="Subject").grid(row=0, column=0, sticky="w")
    ttk.Entry(sources_tab, textvariable=source_subject_var, width=30).grid(row=0, column=1, sticky="ew", padx=6)
    ttk.Label(sources_tab, text="Platform").grid(row=0, column=2, sticky="w")
    ttk.Combobox(
        sources_tab,
        textvariable=source_platform_var,
        values=("x", "linkedin", "youtube", "substack", "reddit", "spotify", "apple_podcasts", "medium", "rss"),
        width=12,
        state="readonly",
    ).grid(row=0, column=3, padx=6)
    ttk.Label(sources_tab, text="Source ID").grid(row=0, column=4, sticky="w")
    ttk.Entry(sources_tab, textvariable=source_id_var, width=24).grid(row=0, column=5, sticky="ew", padx=6)
    ttk.Button(
        sources_tab,
        text="Add Source",
        command=lambda: state.run_cli(
            "add-source",
            "--subject",
            source_subject_var.get().strip(),
            "--platform",
            source_platform_var.get().strip(),
            "--source-id",
            source_id_var.get().strip(),
        ),
    ).grid(row=0, column=6, padx=6)
    ttk.Button(
        sources_tab,
        text="Remove Source",
        command=lambda: state.run_cli(
            "remove-source",
            "--subject",
            source_subject_var.get().strip(),
            "--platform",
            source_platform_var.get().strip(),
            "--source-id",
            source_id_var.get().strip(),
        ),
    ).grid(row=0, column=7, padx=6)

    ttk.Separator(sources_tab, orient=tk.HORIZONTAL).grid(row=1, column=0, columnspan=8, sticky="ew", pady=12)

    ttk.Label(sources_tab, text="Source URL").grid(row=2, column=0, sticky="w")
    ttk.Entry(sources_tab, textvariable=source_url_var, width=80).grid(row=2, column=1, columnspan=5, sticky="ew", padx=6)
    ttk.Button(
        sources_tab,
        text="Add Source URL",
        command=lambda: state.run_cli(
            "add-source-url",
            "--subject",
            source_subject_var.get().strip(),
            "--url",
            source_url_var.get().strip(),
        ),
    ).grid(row=2, column=6, padx=6)
    ttk.Button(
        sources_tab,
        text="List Sources",
        command=lambda: state.run_cli("list-sources", "--subject", source_subject_var.get().strip()),
    ).grid(row=2, column=7, padx=6)
    sources_tab.columnconfigure(1, weight=1)
    sources_tab.columnconfigure(5, weight=1)

    # Preferences tab
    pref_subject_var = tk.StringVar()
    pref_include_var = tk.StringVar()
    pref_exclude_var = tk.StringVar()

    ttk.Label(prefs_tab, text="Subject").grid(row=0, column=0, sticky="w")
    ttk.Entry(prefs_tab, textvariable=pref_subject_var, width=30).grid(row=0, column=1, sticky="ew", padx=6)
    ttk.Button(
        prefs_tab,
        text="Show Preferences",
        command=lambda: state.run_cli("show-preferences", "--subject", pref_subject_var.get().strip()),
    ).grid(row=0, column=2, padx=6)

    ttk.Label(prefs_tab, text="Include terms (comma separated)").grid(row=1, column=0, sticky="w")
    ttk.Entry(prefs_tab, textvariable=pref_include_var, width=70).grid(row=1, column=1, columnspan=2, sticky="ew", padx=6)
    ttk.Label(prefs_tab, text="Exclude terms (comma separated)").grid(row=2, column=0, sticky="w")
    ttk.Entry(prefs_tab, textvariable=pref_exclude_var, width=70).grid(row=2, column=1, columnspan=2, sticky="ew", padx=6)

    def _refine_preferences() -> None:
        args = ["refine-preferences", "--subject", pref_subject_var.get().strip()]
        for term in [t.strip() for t in pref_include_var.get().split(",") if t.strip()]:
            args.extend(["--include", term])
        for term in [t.strip() for t in pref_exclude_var.get().split(",") if t.strip()]:
            args.extend(["--exclude", term])
        state.run_cli(*args)

    ttk.Button(prefs_tab, text="Apply Refinement", command=_refine_preferences).grid(row=3, column=2, padx=6, pady=8, sticky="e")
    prefs_tab.columnconfigure(1, weight=1)

    # Actions tab
    login_platform_var = tk.StringVar(value="x")
    import_subject_var = tk.StringVar()
    import_platform_var = tk.StringVar(value="x")
    import_limit_var = tk.StringVar(value="100")

    ttk.Label(actions_tab, text="Login platform").grid(row=0, column=0, sticky="w")
    ttk.Combobox(
        actions_tab,
        textvariable=login_platform_var,
        values=("x", "linkedin", "youtube", "substack", "medium", "spotify", "apple_podcasts"),
        width=12,
        state="readonly",
    ).grid(row=0, column=1, sticky="w", padx=6)
    ttk.Button(
        actions_tab,
        text="Open Login Flow",
        command=lambda: state.run_cli("login", "--platform", login_platform_var.get().strip()),
    ).grid(row=0, column=2, padx=6)

    ttk.Separator(actions_tab, orient=tk.HORIZONTAL).grid(row=1, column=0, columnspan=6, sticky="ew", pady=10)

    ttk.Label(actions_tab, text="Import follows subject").grid(row=2, column=0, sticky="w")
    ttk.Entry(actions_tab, textvariable=import_subject_var, width=30).grid(row=2, column=1, sticky="ew", padx=6)
    ttk.Label(actions_tab, text="Platform").grid(row=2, column=2, sticky="w")
    ttk.Combobox(
        actions_tab,
        textvariable=import_platform_var,
        values=("x", "linkedin", "youtube", "substack", "medium", "spotify", "apple_podcasts"),
        width=12,
        state="readonly",
    ).grid(row=2, column=3, sticky="w", padx=6)
    ttk.Label(actions_tab, text="Limit").grid(row=2, column=4, sticky="w")
    ttk.Entry(actions_tab, textvariable=import_limit_var, width=8).grid(row=2, column=5, sticky="w", padx=6)
    ttk.Button(
        actions_tab,
        text="Import Follows",
        command=lambda: state.run_cli(
            "import-follows",
            "--subject",
            import_subject_var.get().strip(),
            "--platform",
            import_platform_var.get().strip(),
            "--limit",
            import_limit_var.get().strip() or "100",
        ),
    ).grid(row=2, column=6, padx=6)

    ttk.Separator(actions_tab, orient=tk.HORIZONTAL).grid(row=3, column=0, columnspan=7, sticky="ew", pady=10)
    ttk.Button(actions_tab, text="Run Read Content Once", command=lambda: state.run_cli("run-nightly-once")).grid(row=4, column=0, padx=6, pady=6, sticky="w")
    ttk.Button(actions_tab, text="Run Get Digest Once", command=lambda: state.run_cli("run-digest-once")).grid(row=4, column=1, padx=6, pady=6, sticky="w")

    def _on_close() -> None:
        state.stop_agent()
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", _on_close)
    state.poll_logs()
    root.mainloop()
