# Generating documentation

To generate the [Doxygen](http://doxygen.org) documentation for the
storage engine API, install `doxygen` using your system's package manager, or
[follow the download instructions here](http://www.stack.nl/~dimitri/doxygen/download.html).

The Doxygen configuration file was generated and configured using version 1.8.13.

The following commands will output the html documentation to `docs/html` relative to the root of
this repository.

```
cd docs
doxygen storage.Doxyfile
```

Open `docs/html/index.html` in your web browser to view the generated documentation.
