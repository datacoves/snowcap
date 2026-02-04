import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, Generator, Iterable, TypeVar

T = TypeVar("T")
R = TypeVar("R")

logger = logging.getLogger(__name__)


def execute_in_threads(
    func: Callable[[Any], R],
    items: Iterable[T],
    max_workers: int = 8,
    item_to_args: Callable[[T], Any] = lambda x: [],
    item_to_kwargs: Callable[[T], Dict[str, Any]] = lambda x: {},
    error_handler: Callable[[Exception, T], None] = lambda e, item: None,
) -> Generator[tuple[T, R], None, None]:
    """
    Execute a function in parallel using ThreadPoolExecutor and yield results as they complete.

    Args:
        func: The function to execute for each item
        items: An iterable of items to process
        max_workers: Maximum number of worker threads (default: 8)
        item_to_args: Optional function to transform each item into arguments for func
                     (default: identity function that passes the item directly)
        item_to_kwargs: Optional function to transform each item into keyword arguments for func
                      (default: identity function that passes no keyword arguments)
        error_handler: Optional function to handle exceptions
                      (default: log error and re-raise)

    Yields:
        Tuples of (original_item, result) as they complete

    Example:
        >>> def process_data(data):
        ...     return data * 2
        >>>
        >>> data_items = [1, 2, 3, 4, 5]
        >>> for item, result in execute_in_parallel(process_data, data_items):
        ...     print(f"Item {item} produced result {result}")

    Example with custom argument transformation:
        >>> def fetch_resource(session, urn):
        ...     # Fetch a resource using session and URN
        ...     return {"data": f"Resource {urn}"}
        >>>
        >>> urns = ["urn1", "urn2", "urn3"]
        >>> session = {"token": "abc123"}
        >>>
        >>> # Transform each URN into (session, urn) arguments
        >>> def prepare_args(urn):
        ...     return {"session": session, "urn": urn}
        >>>
        >>> for urn, data in execute_in_parallel(fetch_resource, urns, item_to_args=prepare_args):
        ...     print(f"URN {urn} data: {data}")
    """
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Create a mapping of futures to their original items
        future_to_item = {executor.submit(func, *item_to_args(item), **item_to_kwargs(item)): item for item in items}

        # Yield results as they complete
        for future in as_completed(future_to_item):
            item = future_to_item[future]
            try:
                result = future.result()
                yield item, result
            except Exception as e:
                error_handler(e, item)
