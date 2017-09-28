from functools import wraps


def get_env_key(obj, key=None):
    """
    Return environment variable key to use for lookups within a
    namespace represented by the package name.
    """
    return str.join('_', [obj.__module__.replace('.','_').upper(),
        key.upper()])


def valid_for(validation_key, validation_value):
    def wrapper_decorator(function):
        @wraps(function)
        def wrapper(*args, **kwargs):
            if args[0].__dict__[validation_key] != validation_value:
                raise TypeError(
                    "Function '%s' can only be invoked on a %s of %s"
                    % (function.__name__, validation_key, validation_value))
            function(*args, **kwargs)
        return wrapper
    return wrapper_decorator
