import logging
import functools


def log(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        args_repr = [repr(a) if (type(a) in [str, bool, int, float, list]) else "" for a in args]
        kwargs_repr = [f"{k}={v!r}" if (type(v) in [str, bool, int, float, list]) else "" for k, v in kwargs.items()]
        signature = ", ".join(args_repr + kwargs_repr)
        logging.debug(f"function {func.__name__} called with args {signature}")
        try:
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            logging.exception(f"Exception raised in {func.__name__} called with args {signature}. exception: {str(e)}")
            raise e
    return wrapper