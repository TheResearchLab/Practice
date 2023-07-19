# Chapter 2

- Generator functions will always return generator objects.
- Is the generator an iterator?
- Generators is like a coroutine that has multiple entry points in a function whereas a regular function only has one entry point
- Generators are only as scalable as their least efficient line of code.
- What is the maximum memory footprint and how can I minimize it.
- Yield From is used when you pass a generator to another generator. Ie yield from generator
------------ Chapter 2.4 -------------
- zip,map, and filter all return iterable objects instead of a list or array giving the user access to built in iterable functionality
------------ Chapter 2.5 -------------
- iterators are different from objects that are iterable.

- An object is an iterator if:
        - defines __next__ method with no args
        - each time next is called it returns next item in sequence
        - calls StopIteration after being called
        - also defines boilerplate called __iter__ method. It returns itself.
- An object is an iterable if:
        - it defines an __iter__ method which creates and returns an iterator over the elements in the container or
        - follows the sequence protocol. defines __get-items__ magic method for square brackets and lets you call foo[0], foo[1], until you have passed last index of container.
