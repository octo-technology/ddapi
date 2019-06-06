def is_ipython():
    """
    If it detects any IPython frontend
    (qtconsole, interpreter or notebook)
    :return: True if in ipython context
    """
    try:
        from IPython import get_ipython
        config = get_ipython().config
        return True
    except:
        return False
