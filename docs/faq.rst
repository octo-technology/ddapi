Frequently Asked Question
=========================


ValueError: "Cannot cast DatetimeIndex to dtype datetime64[us]" when trying to import dataframe
-----------------------------------------------------------------------------------------------

This error is due to pandas not being able to cast timezone-aware datetimes into datetime64. We recommend
you avoid localized datetimes and convert to UTC as often as possible. For example, you may want to do :

..code:: python

    import pandas as pd
    datetime_range = pd.date_range(start=..., periods=..., tz='US/Pacific')
    datetime_range = datetime_range.tz_convert("UTC")
    datetime_range = datetime_range.tz_localize(None)