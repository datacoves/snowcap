import logging.config

# __version__ = open("version.md", encoding="utf-8").read().split(" ")[2]

from .blueprint import Blueprint
from .resources import *  # noqa: F403

logger = logging.getLogger("snowcap")


__all__ = [
    "Blueprint",
]

# Datacoves brand colors (24-bit true color ANSI codes)
WHITE = "\033[38;2;232;244;254m"    # #E8F4FE (Light blue/white for snow)
BLUE = "\033[38;2;52;150;224m"      # #3496E0
YELLOW = "\033[38;2;255;209;1m"     # #FFD101
RESET = "\033[0m"

LOGO = f"""
{WHITE}   ___ _ __   _____      _____ __ _ _ __  {WHITE}   ❄  *{RESET}
{BLUE}  / __| '_ \\ / _ \\ \\ /\\ / / __/ _` | '_ \\ {WHITE} *  ▲  ❄{RESET}
{BLUE}  \\__ \\ | | | (_) \\ V  V / (_| (_| | |_) |{WHITE}   ▲▲▲{RESET}
{BLUE}  |___/_| |_|\\___/ \\_/\\_/ \\___\\__,_| .__/ {BLUE}  ▲▲▲▲▲{RESET}
{BLUE}                                   |_|    {BLUE} ▲▲▲▲▲▲▲{RESET}
{YELLOW}  by Datacoves{RESET}
""".strip(
    "\n"
)
