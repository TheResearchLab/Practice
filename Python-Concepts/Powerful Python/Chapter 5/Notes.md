------ Basic Decorator --------
- decorator is a function that takes a function
- bare function is the function that the decorator is applied to
- end result is a decorated function
- adding self to (*args,**kwargs) ie. (self,*args,**kwargs) turns decorator into a decorator that only works with funcs that at least have 1 arg.
- adding the self arg works for class methods
------ Data in Decorators --------
- data can be added in the decorator but outside the wrapper to keep track of data each time the function is called, but data only instantiated once.
- the nonlocal is used in cases where the container for a variable changes in memory. In the case of the variable changing nonlocal is needed because variable change unlike with the dictionary where the dictionaries contents change but not the variable holding the object.

------ Decorators That Take Args --------


------ Method Decorators --------
- Any object can be made callable. Can instantiate functions 
- Class decorators can leverage inheritance.
- Author has strong feelings that the reader should get comfortable with function and object decorators.

------ Class Decorators --------

---- Preserving the Wrapped Function ----
- function objects automatically have attrs like __name__ __doc__ __module__. Wrappers break code relying on them.

- decorators mask wrapped function signature and blocks inspect.getsource()

- decorators can't be applied in class methods or descriptors