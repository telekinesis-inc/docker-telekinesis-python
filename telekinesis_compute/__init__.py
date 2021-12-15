from .manager import AppManager

from pkg_resources import get_distribution

__version__ = get_distribution(__name__).version

__all__ = [
    "__version__",
    "AppManager",
]