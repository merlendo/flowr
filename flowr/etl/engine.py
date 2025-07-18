from typing import Any, Callable, Dict, List, Tuple, Union

import polars as pl
from colorama import Fore, Style, init

init(autoreset=True)


TransformStep = Union[
    Callable[[pl.DataFrame], pl.DataFrame],
    Tuple[Callable[..., pl.DataFrame], Dict[str, Any]],
]

DestinationStep = Union[
    Callable[[pl.DataFrame], int], Tuple[Callable[..., int], Dict[str, Any]]
]

SourceStep = Union[
    Callable[..., pl.DataFrame], Tuple[Callable[..., pl.DataFrame], Dict[str, Any]]
]


# Palette et styles
BRIGHT = Style.BRIGHT
COLOR = {
    "extract": Fore.YELLOW,
    "transform": Fore.MAGENTA,
    "load": Fore.CYAN,
    "success": Fore.GREEN,
    "error": Fore.RED,
}


class ETLFlow:
    def __init__(
        self,
        source_connector: SourceStep,
        transform: List[TransformStep],
        destination_connector: DestinationStep,
    ):
        self.source_connector = source_connector
        self.transform = transform
        self.destination_connector = destination_connector

    def run(self) -> int:
        try:
            # Extract.
            print(f"{BRIGHT}{COLOR['extract']}[EXTRACT]   Extracting data...")
            if isinstance(self.source_connector, tuple):
                fn, kwargs = self.source_connector
                df = fn(**kwargs)
            else:
                df = self.source_connector()
            print(f"{BRIGHT}{COLOR['extract']}[EXTRACT]   Data shape: {df.shape}")

            # Transform.
            for step in self.transform:
                if isinstance(step, tuple):
                    fn, kwargs = step
                    name = fn.__name__
                    print(
                        f"{BRIGHT}{COLOR['transform']}[TRANSFORM] Applying: {name} {kwargs}"
                    )
                    df = fn(df, **kwargs)
                else:
                    name = step.__name__
                    print(f"{BRIGHT}{COLOR['transform']}[TRANSFORM] Applying: {name}")
                    df = step(df)

            # Load.
            print(f"{BRIGHT}{COLOR['load']}[LOAD]      Loading data to destination...")
            if isinstance(self.destination_connector, tuple):
                fn, kwargs = self.destination_connector
                result = fn(df, **kwargs)
            else:
                result = self.destination_connector(df)
            print(f"{BRIGHT}{COLOR['load']}[LOAD]      Rows written: {result}")

            # Success.
            print(
                f"{BRIGHT}{COLOR['success']}[SUCCESS]   ETL pipeline completed successfully"
            )
            return result

        except Exception as e:
            print(f"{BRIGHT}{COLOR['error']}[ERROR]     ETL pipeline failed: {e}")
            raise
