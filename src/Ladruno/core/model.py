# core/model.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from Ladruno.engine.run import Run


@dataclass(frozen=True, slots=True)
class SlurmDefaults:
    """Default SLURM/OpenSees execution settings for all Runs in a Model."""
    number_of_nodes: int = 1
    max_nodes: int = 18
    max_tasks_per_node: int = 32
    opensees_exe: str = "/mnt/nfshare/bin/openseesmp-26062025"
    archive_destination: str = "/mnt/krakenschest/home/nmorabowen"
    verbose: bool = False


class Model:
    """
    A Model represents one simulation folder or a collection of simulation folders
    under a root path (each containing a main.tcl).

    - If root contains main.tcl -> single run
    - Else -> recursively find all */main.tcl and create one Run per parent folder
    """

    def __init__(self, path: str | Path, *, defaults: SlurmDefaults = SlurmDefaults()) -> None:
        self.path: Path = Path(path).expanduser().resolve()
        self.defaults = defaults

        self.runs: list[Run] = self._collect_runs(tcl_name="main.tcl")

        if not self.runs:
            raise FileNotFoundError(f"No main.tcl found in: {self.path}")

    # ----------------------------- discovery ----------------------------- #

    def _collect_runs(self, *, tcl_name: str) -> list[Run]:
        """
        Build all Run objects discovered under self.path.

        Rule:
        - If self.path/main.tcl exists -> only that one
        - Else -> all recursive matches of **/main.tcl
        """
        root_tcl = self.path / tcl_name
        if root_tcl.exists():
            return [self._make_run(self.path)]

        tcl_files = sorted(self.path.rglob(tcl_name))
        return [self._make_run(tcl_path.parent) for tcl_path in tcl_files]

    def _make_run(self, run_folder: Path) -> Run:
        """Factory for Run with shared defaults."""
        d = self.defaults
        return Run(
            folder_path=str(run_folder),
            number_of_nodes=d.number_of_nodes,
            max_nodes=d.max_nodes,
            max_tasks_per_node=d.max_tasks_per_node,
            verbose=d.verbose,
            opensees_exe=d.opensees_exe,
            archive_destination=d.archive_destination,
        )
    
    def _auto_job_name(self, run: Run) -> str:
        """
        Generate a SLURM-friendly job name from the run folder path,
        relative to the model root.
        """
        run_path = Path(run.path).resolve()
        rel = run_path.relative_to(self.path)

        # folder1/folder2 -> folder1_folder2
        name = "_".join(rel.parts)

        # SLURM safety: avoid empty names
        return name or self.path.name


    # ----------------------------- submission ---------------------------- #

    def submit(
        self,
        *,
        archive: bool = False,
        fix: bool = True,
        rebuild: bool = True,
        job_name: str | None = None,
        nodes: int | None = None,
        ntasks: int | None = None,
        ntasks_per_node: int | None = None,
        exclude: list[str] | None = None,
        tcl_file: str = "main.tcl",
        monitor_ram: bool = False,
        monitor_interval: int = 30,
        log_file: str = "memtrack_node.txt",
    ) -> list[str]:
        """
        Submit all runs and return SLURM job IDs.

        Arguments are forwarded to Run.submit().
        """
        self._print_header()

        job_ids: list[str] = []
        for idx, run in enumerate(self.runs, start=1):
            self._print_run_progress(idx, len(self.runs), run)

            resolved_job_name = job_name or self._auto_job_name(run)

            job_id = run.submit(
                archive=archive,
                fix=fix,
                rebuild=rebuild,
                job_name=resolved_job_name,
                nodes=nodes,
                ntasks=ntasks,
                ntasks_per_node=ntasks_per_node,
                exclude=exclude,
                tcl_file=tcl_file,
                monitor_ram=monitor_ram,
                monitor_interval=monitor_interval,
                log_file=log_file,
            )
            job_ids.append(job_id)

        self._print_footer(len(job_ids))
        return job_ids

    # ----------------------------- printing ----------------------------- #

    def _print_header(self) -> None:
        if self.defaults.verbose:
            print(f"\n🚀 Submitting {len(self.runs)} model(s)\n")

    def _print_run_progress(self, i: int, total: int, run: Run) -> None:
        if self.defaults.verbose:
            # run.path is presumably a Path in your Run object; if it's a string, adjust accordingly
            name = getattr(run, "path", None)
            name = name.name if hasattr(name, "name") else str(name) if name is not None else "<run>"
            print(f"[{i}/{total}] {name}")

    def _print_footer(self, total: int) -> None:
        if self.defaults.verbose:
            print(f"\n✅ {total} job(s) submitted")
            print("LARGA VIDA AL LADRUÑO!!!\n")
