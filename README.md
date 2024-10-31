# SQLite

This is a reimplementation of a tiny subset of sqlite functionality.
It is a project for learning in python. Given that python is guaranteed
to be slow for this type of application, why us it for the project? 
I've done lots of data-science type work in this language, but never
tried to use it for actual software. Since the product will never be 
used anyway, it seemed like a good opportunity. 

One pattern that I miss from other languages is algebraic data types. 
For example, in the record header of a sqlite database leaf node, 
you read consecutive varints, each of which corresponds to a different
serial type. The return value of this function should be a type which 
corresponds to the serial type, but I am resorting to something like
`("int", 4)` which means a 4 byte integer. I don't like using strings
this way.

This project is really about
- can you work with binary data?
- can you follow complex documentation?
