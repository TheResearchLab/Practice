---------- Configuring the basic interface ------
- Python logging uses python 2 formattion %(paramName)s
- it can be changed but may be too much effort.

--------- Logging to multiple destinations -------
- a handler can only make itself more select than its logger not less
- logger is autoset to warning so without change this will block everything besides warnings and errors
- you can implement your own subclass of logging handlers